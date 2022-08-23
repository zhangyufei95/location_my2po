CREATE TABLE `ht_travel`.`travel_order_external_cost` (
                                              `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                              `main_order_id` bigint(20) DEFAULT NULL COMMENT '主订单ID',
                                              `status` int(4) NOT NULL DEFAULT '0' COMMENT '0代表禁用，1代表可用',
                                              `travel_order_id` varchar(32) NOT NULL DEFAULT '' COMMENT '行程单ID',
                                              `real_gascard_id` varchar(1024) DEFAULT NULL COMMENT '实收油卡, 卡号为加油卡管理模块中的卡号, 多张加油卡用英文逗号分隔, 例如:1000113300015396630,1000113300015396631',
                                              `real_gascard_money` decimal(9,3) DEFAULT '0.000' COMMENT '实收油卡金额(元), 实收油卡初始金额总和',
                                              `real_cash` decimal(9,3) DEFAULT '0.000' COMMENT '实收现金(元), 云柚实收管理模块中对应行程单的实收金额总和',
                                              `real_other_money` decimal(9,3) DEFAULT '0.000' COMMENT '实收其他(元)',
                                              `real_other_money_note` varchar(64) DEFAULT NULL COMMENT '实收其他备注',
                                              `price_forcar` decimal(9,3) DEFAULT '0.000' COMMENT '外请车价格(元)',
                                              `agency_cost_forcar` decimal(9,3) DEFAULT '0.000' COMMENT '外请信息费(元)',
                                              `gascard_pay_money` decimal(9,3) DEFAULT '0.000' COMMENT '支付油卡金额(元)',
                                              `gascard_pay_money_status` int(1) DEFAULT '0' COMMENT '支付油卡金额的状态, 0:未支付, 1:已支付',
                                              `pay_gascard_id` varchar(64) DEFAULT NULL COMMENT '支付油卡卡号',
                                              `pre_pay_money` decimal(9,3) DEFAULT '0.000' COMMENT '支付预付款(元)',
                                              `pre_pay_money_status` int(1) DEFAULT '0' COMMENT '预付款的支付状态, 0:未支付, 1:已申请支付, 2:支付完成',
                                              `unloaded_pay_money` decimal(9,3) DEFAULT '0.000' COMMENT '支付到付款(元), 以卸货完成为标准支付到付款',
                                              `unloaded_pay_money_status` int(1) DEFAULT '0' COMMENT '到付款的支付状态, 0:未支付, 1:已申请支付, 2:支付完成',
                                              `sheet_pay_money` decimal(9,3) DEFAULT '0.000' COMMENT '支付回单款(元)',
                                              `sheet_pay_money_status` int(1) DEFAULT '0' COMMENT '回单款的支付状态, 0:未支付, 1:已申请支付, 2:支付完成',
                                              `main_pay_method` int(1) DEFAULT '0' COMMENT '三款支付方式(预付款, 到付款, 回单款), 0:银行付款, 1:运满满',
                                              `main_pay_ymm_no` varchar(32) DEFAULT NULL COMMENT '三款支付方式为运满满时, 运满满的订单号',
                                              `other_pay_money` decimal(9,3) DEFAULT '0.000' COMMENT '支付其它款(元)',
                                              `other_pay_money_status` int(1) DEFAULT '0' COMMENT '其它款的支付状态, 0:未支付, 1:已申请支付, 2:支付完成',
                                              `other_pay_method` int(1) DEFAULT '0' COMMENT '其它款的支付方式, 0:银行付款, 1:运满满',
                                              `other_pay_ymm_no` varchar(32) DEFAULT NULL COMMENT '其它款的支付方式为运满满时, 运满满的订单号',
                                              `other_pay_note` varchar(64) DEFAULT NULL COMMENT '支付其它款备注',
                                              `contract_type` int(4) DEFAULT '1' COMMENT '承运合同类型 1:云柚外请合同 2:不签合同 3:撮合成交合同',
                                              `contract_note` varchar(32) DEFAULT NULL COMMENT '承运合同备注',
                                              `recipient_name` varchar(64) DEFAULT NULL COMMENT '收款人',
                                              `recipient_phone` varchar(64) DEFAULT NULL COMMENT '收款人电话',
                                              `payee_account` varchar(32) DEFAULT NULL COMMENT '收款账号(银行卡号)',
                                              `payee_account_card_no` varchar(32) DEFAULT NULL COMMENT '开户证件号',
                                              `payee_account_bank` varchar(32) DEFAULT NULL COMMENT '开户行',
                                              `bank_code` varchar(10) NOT NULL DEFAULT '' COMMENT '银行英文缩写',
                                              `bank_name` varchar(32) NOT NULL DEFAULT '' COMMENT '提现开户行',
                                              `other_bank` varchar(32) NOT NULL DEFAULT '' COMMENT '其他银行',
                                              `bank_geo_id` bigint(20) DEFAULT NULL COMMENT '开户行所在地',
                                              `damaged_goods_cut_fine` decimal(9,3) DEFAULT '0.000' COMMENT '货损扣款(元)',
                                              `damaged_goods_by_company` decimal(9,3) DEFAULT '0.000' COMMENT '云柚承担货损扣款(元)',
                                              `supplier_cut_insure` decimal(9,3) NOT NULL DEFAULT '0.000' COMMENT '货主扣款-货物险',
                                              `supplier_cut_late_fine` decimal(9,3) DEFAULT '0.000' COMMENT '货主扣款-迟到罚款(元)',
                                              `heptax_cut_late_fine` decimal(9,3) DEFAULT '0.000' COMMENT '货主扣款-迟到罚款, 云柚承担的部分(元)',
                                              `supplier_cut_other_money` decimal(9,3) DEFAULT '0.000' COMMENT '货主扣款-其他费用(元)',
                                              `other_money_by_company` decimal(9,3) DEFAULT '0.000' COMMENT '货主扣款-其他费用中由云柚承担的部分(元)',
                                              `supplier_cut_other_note` varchar(800) DEFAULT '' COMMENT '货主扣款-其他费用备注信息',
                                              `cut_late_fine` decimal(9,3) DEFAULT '0.000' COMMENT '云柚迟到罚款(元)',
                                              `cut_fine_notes` varchar(255) DEFAULT '' COMMENT '云柚迟到罚款备注',
                                              `heptax_cut_other_money` decimal(9,3) DEFAULT '0.000' COMMENT '云柚其他扣款(元)',
                                              `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '外请车订单创建时间',
                                              `create_users_id` bigint(20) DEFAULT NULL,
                                              `update_users_id` bigint(20) DEFAULT NULL,
                                              `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                              `price_pay_forcar` decimal(9,3) DEFAULT '0.000' COMMENT '外请车实付价格(元)',
                                              `trade_type` mediumint(9) DEFAULT '0' COMMENT '交易类型  0 云柚成交  1 云柚撮合',
                                              `referral_fee` decimal(9,3) DEFAULT '0.000' COMMENT '云柚撮合 三方介绍费',
                                              `difference_fee` decimal(9,3) DEFAULT '0.000' COMMENT '云柚成交 差价费',
                                              `wait_receipt` decimal(9,2) DEFAULT '0.00' COMMENT '待收款金额(元)',
                                              `wait_receipt_note` varchar(100) DEFAULT '' COMMENT '待收款备注',
                                              `rec_agency_price` decimal(9,3) DEFAULT '0.000' COMMENT '云柚应收信息费',
                                              `deposit_gas_card` decimal(11,2) DEFAULT NULL COMMENT '油卡押金',
                                              `deposit_damaged_goods` decimal(11,2) DEFAULT NULL COMMENT '货损押金',
                                              `deposit_receipt` decimal(11,2) DEFAULT NULL COMMENT '回单押金',
                                              `payee_account_type` int(1) DEFAULT '0' COMMENT '账户类型 0 非对公 1对公',
                                              `cnaps_code` varchar(64) NOT NULL DEFAULT '' COMMENT '支行行号 对公账号',
                                              `dispatch_channel` int(4) DEFAULT '0' COMMENT '调车渠道 详情见枚举',
                                              `settle_method` int(4) DEFAULT '1' COMMENT '给司机运费计算方式 1整车 2 计量',
                                              `unit_price_for_car` decimal(11,3) DEFAULT '0.000' COMMENT '计量单位支付给司机',
                                              `receipt_deposit` decimal(11,3) DEFAULT '0.000' COMMENT '回单押金',
                                              `profit` decimal(11,3) DEFAULT NULL COMMENT '（预估）利润',
                                              `reason_for_car` varchar(500) DEFAULT NULL COMMENT '请车价格理由',
                                              `pay_way` int(4) NOT NULL DEFAULT '1' COMMENT '支付方式{1:线上，2:线下}',
                                              `invoicing_type` int(4) NOT NULL DEFAULT '0' COMMENT '开票类型 0 不开票     1 专票  2普票',
                                              `relation_travel_id` bigint(20) DEFAULT NULL COMMENT '行程单主键id',
                                              `relation_order_id` varchar(32) DEFAULT NULL COMMENT '关联行程单id',
                                              `relation_create_time` datetime DEFAULT NULL COMMENT '关联创建时间',
                                              `relation_update_time` datetime DEFAULT NULL COMMENT '关联修改时间',
                                              `relation_create_user_id` bigint(20) DEFAULT NULL COMMENT '关联创建人',
                                              `relation_update_user_id` bigint(20) DEFAULT NULL COMMENT '关联修改人',
                                              PRIMARY KEY (`id`),
                                              KEY `INX_OID` (`travel_order_id`),
                                              KEY `IDX_TSW` (`travel_order_id`,`status`,`wait_receipt`),
                                              KEY `IDX_MID` (`main_order_id`),
                                              KEY `INX_RE_TID` (`relation_travel_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=106657 DEFAULT CHARSET=utf8mb4 COMMENT='外请车行程单费用信息表'