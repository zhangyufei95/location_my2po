CREATE TABLE `ht_travel`.`travel_order` (
                                            `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                            `main_order_id` bigint(20) DEFAULT NULL COMMENT '主订单ID',
                                            `order_id` varchar(32) DEFAULT NULL COMMENT '行程单号',
                                            `order_sn` varchar(32) DEFAULT NULL COMMENT '行程单的序列号, 如:豫LJ7709_0000104',
                                            `status` int(4) NOT NULL DEFAULT '0' COMMENT '0代表禁用，1代表启用',
                                            `mode_type` int(4) DEFAULT '0' COMMENT '合作模式，0:老模式, 1:天蝎1.0, 2:天蝎2.0, 3:天蝎3.0, 4:天蝎4.0',
                                            `pre_status` int(4) DEFAULT '0',
                                            `external` int(4) DEFAULT '0' COMMENT '0不是外请，1是外请',
                                            `order_type` int(4) NOT NULL DEFAULT '0' COMMENT '''0:订单 1:行程单 2:订单和行程单都可以展示 3:行程单展示'' ',
                                            `line_id` bigint(20) DEFAULT NULL COMMENT '打单线路ID',
                                            `goodpath_idx` int(4) DEFAULT NULL COMMENT '所在线路的最优路径序号, 从1开始',
                                            `line_distance` decimal(9,3) DEFAULT '-1.000' COMMENT '根据行程单起始地址, 可能包含最优路径途径点的高德货车导航距离',
                                            `dispatcher` varchar(32) DEFAULT NULL COMMENT '调度人员的名称',
                                            `dispatcher_note` varchar(128) DEFAULT NULL COMMENT '调度派单备注',
                                            `order_source` int(4) NOT NULL DEFAULT '0' COMMENT '行程单来源, 0:打单, 1:询价, 2:其他',
                                            `order_pay_type` int(4) NOT NULL DEFAULT '0' COMMENT '订单结算方式, 0: 差额结算, 1: 全额结算',
                                            `order_status` int(11) NOT NULL DEFAULT '0' COMMENT '行程单状态, 0-16, 见 Constants.java',
                                            `is_payed` int(4) NOT NULL DEFAULT '0' COMMENT '行程单是否已结算完成, 0:没有 1:结算完成',
                                            `goods_type` int(4) DEFAULT '0' COMMENT '货物类型,  0:普货, 1:快递货, 2:工厂货, 3:快运',
                                            `total_weight` decimal(10,4) NOT NULL DEFAULT '0.0000' COMMENT '货物总重(吨)',
                                            `truck_goods_weight` decimal(9,3) NOT NULL DEFAULT '0.000' COMMENT '车货总重(吨)',
                                            `weight_with_truck` int(4) DEFAULT '0' COMMENT '1 表示随车装 0 表示称重',
                                            `saler` varchar(32) DEFAULT NULL COMMENT '订单销售',
                                            `weighed_pic_is_clear` int(4) NOT NULL DEFAULT '0' COMMENT '过磅照片是否清晰, 0: 未知, 1: 清晰, 2: 不清晰',
                                            `truck_column` int(4) DEFAULT NULL COMMENT '0代表不拆，1代表拆',
                                            `remove_column_num` int(4) DEFAULT '0' COMMENT '拆立柱根数',
                                            `board_type` int(2) DEFAULT NULL COMMENT '1代表直板，2代表高低板，0代表均可',
                                            `load_times` int(11) NOT NULL DEFAULT '0' COMMENT '装货次数',
                                            `unload_times` int(11) DEFAULT '0' COMMENT '卸货次数',
                                            `current_unload_times` int(11) DEFAULT '0' COMMENT '配合状态，当前是第几卸载',
                                            `current_load_times` int(11) DEFAULT '0' COMMENT '配合状态，当前是第几装',
                                            `customer_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '对应的客户表Id',
                                            `route_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '对应的线路id',
                                            `supplier_info` varchar(50) DEFAULT NULL COMMENT '货主信息(顺丰快递, 富士康)',
                                            `goods_info` varchar(50) DEFAULT NULL COMMENT '货品信息(快递,酒水 etc)',
                                            `supplier_contact` varchar(50) DEFAULT NULL COMMENT '货源联系人(姓名手机号)',
                                            `supplier_priority` int(4) DEFAULT '0' COMMENT '货源质量, 0:没有货主质量一般, 1:优质货源, UI要加星号',
                                            `truck_id` bigint(20) DEFAULT NULL COMMENT '承载车辆Id',
                                            `car_type` int(4) unsigned DEFAULT '4' COMMENT '车型, 0: 未知 1: 17.5/16.5m厢车，2: 9.6m厢车, 3: 17.5高栏车, 4: 13米高栏车, 5: 17.5平板, 6: 13.5厢车, 7: 9.6米高栏',
                                            `trailer_id` bigint(20) DEFAULT NULL COMMENT '承载车辆挂车ID',
                                            `truck_contact` varchar(50) DEFAULT NULL COMMENT '车辆联系人(姓名手机号)',
                                            `arrive_time` datetime DEFAULT NULL COMMENT '要求靠台时间, 即调度打单的靠台时间',
                                            `arrive_time_real` datetime DEFAULT NULL COMMENT '真实靠台时间',
                                            `finish_time` datetime DEFAULT NULL COMMENT '行程结束时间',
                                            `is_valid` int(4) DEFAULT '0' COMMENT '整体行程单是否有效 1：有效, -1:无效',
                                            `is_executed` int(1) NOT NULL DEFAULT '1' COMMENT '该单是否被执行, 0: 被放空, 1: 已执行',
                                            `note` varchar(100) DEFAULT NULL COMMENT '行程单备注',
                                            `scheduler_updated_time` varchar(32) DEFAULT NULL COMMENT '定时任务的更新时间 yyyy-MM-dd HH:mm:ss',
                                            `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '行程单创建时间',
                                            `create_users_id` bigint(20) DEFAULT NULL,
                                            `update_users_id` bigint(20) DEFAULT NULL,
                                            `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                            `truck_group` int(11) DEFAULT '0',
                                            `supplier_mobile` varchar(50) DEFAULT NULL,
                                            `customer_route_id` bigint(20) DEFAULT NULL COMMENT '货源池客户对应路线',
                                            `customer_type` int(4) DEFAULT NULL COMMENT '1固定客户 2散单货源池客户',
                                            `car_length` decimal(10,2) DEFAULT NULL COMMENT '车长',
                                            `demand_id` bigint(20) DEFAULT NULL COMMENT '需求id',
                                            `truck_owner_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '车主id',
                                            `driver_grab` int(4) NOT NULL DEFAULT '0' COMMENT '是否为司机抢单 1是0否',
                                            `weight_unit` int(4) DEFAULT NULL COMMENT '货重单位',
                                            `package_way` varchar(100) DEFAULT NULL COMMENT '其他包装方式',
                                            `package_way_code` int(4) DEFAULT NULL COMMENT '包装方式',
                                            `saler_note` varchar(500) DEFAULT NULL COMMENT '商务备注',
                                            `driving_mode` int(4) DEFAULT '0' COMMENT '驾驶模式 1: 单驾 2: 双驾',
                                            `customer_manager_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '客户经理id',
                                            `saler_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '销售id',
                                            `dispatcher_id` bigint(20) DEFAULT '0' COMMENT '调度id',
                                            `region_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '销售所属区域id',
                                            `customer_group` int(10) DEFAULT '0' COMMENT '客户分组',
                                            `acq_user_id` bigint(20) DEFAULT '0' COMMENT '拉新销售id',
                                            `is_vip` int(1) NOT NULL DEFAULT '0' COMMENT '会员标志    MemberType.java 会员枚举',
                                            `region_group_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '区域小组id',
                                            `foreign_id` bigint(20) DEFAULT NULL COMMENT 'IM映射表主键id',
                                            `fn_valid_status` int(4) DEFAULT '0' COMMENT '网络货运平台运单上传校验状态 0.未知 1.正常开票 2.不可开票',
                                            `order_son_status` int(4) DEFAULT NULL COMMENT '子状态',
                                            `fn_flag` int(4) DEFAULT '0' COMMENT '网络货运标记',
                                            `fn_create_users_id` bigint(20) DEFAULT NULL COMMENT '网络货运用户id',
                                            `recommend_mobile` varchar(100) DEFAULT NULL COMMENT '推荐手机号',
                                            `recommend_foreign_id` bigint(20) DEFAULT NULL COMMENT '推荐微信id',
                                            `order_owner_status` int(4) DEFAULT NULL COMMENT '货主侧状态',
                                            `ori_order_owner_status` int(4) DEFAULT NULL COMMENT '货主侧原状态',
                                            `consignor_goods_type` int(4) DEFAULT NULL COMMENT '货物类型，枚举：ConsignorGoodsType',
                                            `main_body_type` int(4) DEFAULT '1' COMMENT '签约主体类型：1山西2黑龙枚举CustomerMainBodyEnum',
                                            `captain_id` bigint(20) DEFAULT NULL COMMENT '车队长id',
                                            PRIMARY KEY (`id`) USING BTREE,
                                            KEY `IDX_MID` (`main_order_id`),
                                            KEY `IDX_OID` (`order_id`),
                                            KEY `IDX_STATUS_E` (`status`,`external`),
                                            KEY `IDX_TRUCK_TIME` (`truck_id`,`create_time`) USING BTREE,
                                            KEY `IDX_CTIME` (`create_time`),
                                            KEY `IDX_DID` (`demand_id`) USING BTREE,
                                            KEY `IDX_SID` (`saler_id`) USING BTREE,
                                            KEY `IDX_CID` (`customer_id`) USING BTREE,
                                            KEY `IDX_CRID` (`customer_route_id`),
                                            KEY `IDX_C_ID` (`captain_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=139508 DEFAULT CHARSET=utf8mb4 COMMENT='行程单表'