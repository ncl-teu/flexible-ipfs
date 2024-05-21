set HOME=.
set IPFS_HOME=.ipfs
java -cp .;lib/ipfs-ncl.jar;lib/ipfslib.jar;lib/jackson-core-2.15.2.jar;lib/jackson-databind-2.15.2.jar;lib/commons-math-2.0.jar org.peergos.APIServer Addresses.API /ip4/127.0.0.1/tcp/5001
pause;