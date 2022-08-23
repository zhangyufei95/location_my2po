CREATE TABLE `ht_user`.`base_geo_info` (
  `id` bigint(20) NOT NULL,
  `parent_id` bigint(20) NOT NULL DEFAULT '0',
  `name` varchar(32) NOT NULL COMMENT '名称',
  `pinyin` varchar(100) NOT NULL COMMENT '拼音',
  `lng` decimal(9,6) DEFAULT NULL COMMENT '中心点经度',
  `lat` decimal(9,6) DEFAULT NULL COMMENT '中心点纬度',
  `type` int(11) NOT NULL COMMENT '类型：1代表大区，2代表省份，3代表城市，4代表区县，5代表街道、乡、镇',
  `status` int(4) NOT NULL DEFAULT '0' COMMENT '0代表禁用，1代表可用',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `sort` int(4) NOT NULL DEFAULT '0' COMMENT '排序',
  PRIMARY KEY (`id`),
  KEY `IDX_PID` (`parent_id`),
  KEY `IDX_TYPE` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全国行政区划'