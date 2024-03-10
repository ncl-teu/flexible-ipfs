cd $HOME/nabu-master
export HOME=.
export IPFS_HOME=.ipfs
java -cp .:classes/production/nabu-master:lib/ipfslib.jar:lib/jackson-core-2.15.2.jar:lib/jackson-databind-2.15.2.jar:lib/commons-math-2.0.jar org.ncl.kadrtt.core.cmds.ControlMain
