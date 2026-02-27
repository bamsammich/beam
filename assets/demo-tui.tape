# VHS tape for beam TUI mode demo
# Run: vhs assets/demo-tui.tape

Output assets/demo-tui.gif

Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 800
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set Padding 20

# Pre-create test data
Hide
Type "rm -rf /tmp/beam-demo-src /tmp/beam-demo-dst && mkdir -p /tmp/beam-demo-src/{videos,photos,docs}"
Enter
Sleep 500ms
# ~300 MB of large files
Type "for s in 128 96 64 32; do dd if=/dev/urandom of=/tmp/beam-demo-src/videos/clip_${s}MB.mp4 bs=1M count=$s 2>/dev/null; done"
Enter
Sleep 6s
# 3K small files
Type "for i in $(seq 1 3000); do dd if=/dev/urandom of=/tmp/beam-demo-src/photos/IMG_$i.jpg bs=4K count=1 2>/dev/null; done"
Enter
Sleep 5s
Type "for i in $(seq 1 500); do dd if=/dev/urandom of=/tmp/beam-demo-src/docs/doc_$i.txt bs=1K count=1 2>/dev/null; done"
Enter
Sleep 2s
Type "export PATH=$PWD/bin:$PATH"
Enter
Sleep 500ms
Type "clear"
Enter
Sleep 500ms
Show

Type "beam -a --tui --bwlimit 30M /tmp/beam-demo-src /tmp/beam-demo-dst"
Sleep 500ms
Enter

# Watch feed view with in-flight files
Sleep 8s

# Switch to rate view
Type "r"
Sleep 5s

# Switch back to feed view
Type "r"
Sleep 3s

# Quit TUI
Type "q"
Sleep 2s

# Clean up
Hide
Type "rm -rf /tmp/beam-demo-src /tmp/beam-demo-dst"
Enter
Sleep 500ms
