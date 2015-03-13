# pcopy
Parallel copying and checksumming

Speed up large copies between different storage volumes with this script. 

It copies data in multiple streams and check (with sha1) that source file and copy are identical. 
It supposes that the source and destination are mountpoints on the system running the script. Typical use case: migrate data from an old server to a new one, using a NFS share.


