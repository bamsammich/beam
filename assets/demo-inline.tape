# VHS tape for beam inline mode demo
# Run: vhs assets/demo-inline.tape

Output assets/demo-inline.gif

Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set Padding 20

# Pre-create test data on tmpfs, destination on real disk (cross-device forces
# userspace copy path so --bwlimit actually works)
Hide
Type "rm -rf /tmp/beam-demo ~/beam-demo-dst && mkdir -p /tmp/beam-demo/src/videos /tmp/beam-demo/src/photos"
Enter
Sleep 500ms
# ~500 MB of large files
Type "for i in 1 2 3 4; do dd if=/dev/urandom of=/tmp/beam-demo/src/videos/clip_$i.mp4 bs=1M count=128 2>/dev/null; done"
Enter
Sleep 8s
# 5K small files for rate mode
Type "for i in $(seq 1 5000); do dd if=/dev/urandom of=/tmp/beam-demo/src/photos/IMG_$i.jpg bs=4K count=1 2>/dev/null; done"
Enter
Sleep 8s
Type "export PATH=$PWD/bin:$PATH"
Enter
Sleep 500ms
Type "clear"
Enter
Sleep 500ms
Show

# Cross-device: tmpfs → ext4 — bwlimit will work
Type "beam -a --bwlimit 50M /tmp/beam-demo/src ~/beam-demo-dst"
Sleep 500ms
Enter

# Wait for transfer (~500MB at 50MB/s = ~10s + small files)
Sleep 15s

# Linger on completion summary
Sleep 3s

# Clean up
Hide
Type "rm -rf /tmp/beam-demo ~/beam-demo-dst"
Enter
Sleep 500ms
