# deploy.py (at repo root)
import sys
from deploy.deployer import main

if __name__ == "__main__":
    sys.argv = [sys.argv[0], "--all"]
    main()
