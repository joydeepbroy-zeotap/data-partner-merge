package com.zeotap.merge.dp.poc

case class UserProfile(Common_TS: String,
                       Interest_IAB: String,
                       Demographic_MinAge: Int,
                       Demographic_MaxAge: Int,
                       Demographic_Gender: String,
                       brands: Array[String],
                       IdStore_TS: String,
                       cel_ts: Long,
                       Device_DeviceOS: String,
                       Common_DataPartnerID: Int,
                       CREATED_TS: Long,
                       cookies: Map[String, String]
                      )
