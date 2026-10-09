#!/bin/zsh
cd -- "${0:A:h}/.." || exit 1
exec python3 codex_sse_watch.py --web -n 200 "$@"
