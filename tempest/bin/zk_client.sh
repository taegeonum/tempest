# It can be used for adding/deleting timescales dynamically. 
# zkAddress: zookeeper address
# zkIdentifier: an identifier of MTS window operator. 
# w: window size
# i: interval size
# type: addtion/deletion
java -cp ./target/tempest-0.11-SNAPSHOT.jar org.edu.snu.tempest.operator.window.mts.signal.impl.ZkMTSSignalCommandLine --zkAddress=$1 --zkIdentifier=$2 --w=$3 --i=$4 --type=$5
