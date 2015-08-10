# It can be used for adding/deleting timescales dynamically. 
# zkAddress: zookeeper address
# mts_identifier: an identifier of MTS window operator.
# w: window size
# i: interval size
# type: addtion/deletion
java -cp ./target/tempest-0.11-SNAPSHOT.jar edu.snu.tempest.signal.window.timescale.MTSSignalSender --zkAddress=$1 --mts_identifier=$2 --w=$3 --i=$4 --type=$5
