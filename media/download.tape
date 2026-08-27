# Record media/download.gif   ->   vhs media/download.tape
# Shows xget fetching a file in parallel chunks with its live segmented bar.
Output media/download.gif
Set FontSize 16
Set Width 1300
Set Height 240
Set Padding 14

# Put the freshly built binary on PATH without showing the plumbing.
Hide
Type "export PATH=$PWD/target/release:$PATH"
Enter
Type "clear"
Enter
Show

Type "xget https://proof.ovh.net/files/1Mb.dat /tmp/xget-demo.dat -f"
Sleep 600ms
Enter
Sleep 9s
