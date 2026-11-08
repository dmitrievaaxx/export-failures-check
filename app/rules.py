"""The rule-based forecast (baseline) and the words of an alert: reasons and what to check.

The baseline is a reviewer's rule: a few conditions on the source's recent signals and the flow's
last run, no training. The reasons are the features that stand out for this run, in words; the
recommendation follows the first of them. Neither claims a cause: they say what to look at.
"""

NEVER = 1e5


def baseline(f: dict) -> tuple[int, int]:
    def v(name: str, default: float = 0.0) -> float:
        value = f.get(name)
        return default if value is None else value

    error = v("unst1") >= 2 or v("last_code") == 4 or v("retry1") >= 2 or v("srcerr1") >= 2
    sla = v("warn1") >= 2 or v("last_late") == 1 or v("since_lag", NEVER) <= 5 or v("last_code") == 4
    return 1, 1


# (feature, is it notable, reason, what to check) — in order of how much they say
SIGNS = [
    ("last_code", lambda x: x == 4, "прошлый запуск упал с SCHEMA_MISMATCH: у источника могла смениться схема",
     "Сверьте схему источника с ожидаемой и проверьте, исправлена ли выгрузка после прошлого падения"),
    ("unst1", lambda x: x >= 1, "источник нестабилен: {} предупреждений source_unstable за час",
     "Проверьте доступность и время ответа источника; при необходимости перенесите запуск"),
    ("srcerr1", lambda x: x >= 1, "у других выгрузок того же источника {} ошибок за час",
     "Посмотрите ошибки соседних выгрузок источника — проблема может быть общей"),
    ("lag1", lambda x: x >= 1, "источник медленный: {} предупреждений source_lag за час",
     "Проверьте нагрузку источника и заложите запас времени до срока SLA"),
    ("running", lambda x: x == 1, "предыдущий запуск этой выгрузки ещё не закончился",
     "Проверьте предыдущий запуск: этот встанет в очередь за ним"),
    ("oom3", lambda x: x >= 1, "в последних запусках было OUT_OF_MEMORY ({} раз)",
     "Проверьте объём данных и лимит памяти выгрузки"),
    ("allerr1", lambda x: x >= 3, "за час {} ошибок у разных выгрузок",
     "Проверьте доступность приёмника данных — сбои идут у многих выгрузок сразу"),
    ("ratio_last", lambda x: x >= 0.85, "прошлый запуск занял {:.0%} окна SLA",
     "Проверьте длительность шагов прошлого запуска: запас до срока почти исчерпан"),
    ("last_late", lambda x: x == 1, "прошлый запуск опоздал к сроку",
     "Проверьте, ушла ли причина прошлого опоздания"),
    ("month_end", lambda x: x == 1, "конец месяца: у части выгрузок объём больше обычного",
     "Проверьте объём данных к выгрузке и время на загрузку"),
]


def words(f: dict) -> tuple[list[str], str]:
    reasons, actions = [], []
    for name, notable, reason, action in SIGNS:
        value = f.get(name)
        if value is not None and notable(value):
            reasons.append(reason.format(int(value) if "{}" in reason else value))
            actions.append(action)
        if len(reasons) == 3:
            break
    if not reasons:
        reasons, actions = ["модель видит повышенный риск по истории выгрузки"], [
            "Проверьте последние запуски выгрузки и состояние источника"
        ]
    return reasons, actions[0]
