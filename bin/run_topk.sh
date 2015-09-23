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

# run WordCount test topology
# parameter: a file path containing test parameters

SELF_JAR='./target/tempest-0.11-SNAPSHOT.jar'
TARGET_CLASS='evaluation.example.topk.TopkTestTopology'

CMD="java -Xms32000m -Xmx56000m -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS `cat $1`"
echo $CMD
$CMD

