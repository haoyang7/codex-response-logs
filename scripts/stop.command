#!/bin/zsh
cd -- "${0:A:h}/.." || exit 1
python3 codex_sse_watch.py --stop "$@"
viewer_exit_code=$?
printf '\n按回车关闭此窗口。'
read -r
exit "$viewer_exit_code"
