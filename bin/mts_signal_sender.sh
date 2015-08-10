#!/bin/sh
#
# Copyright (C) 2015 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# It can be used for adding/deleting timescales dynamically.
# zkAddress: zookeeper address
# mts_identifier: an identifier of MTS window operator.
# w: window size
# i: interval size
# type: addtion/deletion
java -cp ./target/tempest-0.11-SNAPSHOT.jar edu.snu.tempest.signal.window.timescale.MTSSignalSender --zkAddress=$1 --mts_identifier=$2 --w=$3 --i=$4 --type=$5
