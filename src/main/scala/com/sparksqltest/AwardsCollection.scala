/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.sparksqltest

import scala.collection.JavaConversions._

class AwardsCollection (
                         val awardid: String,
                         val playerSet: Set[String] ,
                         val playerList: List [String] ,
                         val playerMap: Map[String, Long]) extends Serializable{

  def +(awardCollection:AwardsCollection): AwardsCollection = {
    if (awardid != awardCollection.awardid)
      throw new RuntimeException(s"cannot add awardids that do not match: current is $awardid and attempted to add ${awardCollection.awardid}")
    val inPlayerSet = awardCollection.playerSet
    val inPlayerList = awardCollection.playerList
    val inPlayerMap = awardCollection.playerMap
    new AwardsCollection(
      awardid,
      inPlayerSet ++ playerSet,
      inPlayerList ++ playerList,
      inPlayerMap ++ playerMap
    )
  }
}
