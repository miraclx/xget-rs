# Record media/download.gif   ->   vhs media/download.tape
Output media/download.gif
Set FontSize 16
Set Width 1200
Set Height 300
Set Padding 16
Hide
Type "export PATH=$PWD/target/release:$PATH"
Enter
Type "clear"
Enter
Show
Type "xget https://proof.ovh.net/files/1Mb.dat /tmp/rg.dat -f -s sha256"
Sleep 600ms
Enter
Sleep 15s
