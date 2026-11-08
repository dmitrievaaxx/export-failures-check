import sys
from pathlib import Path

# the solution's own `app`, whatever folder pytest is started from
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
