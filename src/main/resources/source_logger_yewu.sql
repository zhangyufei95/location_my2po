CREATE TABLE `algo_nlp`.`logger_yewu` (
                                          `id` int(11) NOT NULL AUTO_INCREMENT,
                                          `logger_name` longtext ,
                                          `level` varchar(255) DEFAULT NULL,
                                          `level_no` int(11) DEFAULT NULL,
                                          `output_msg` longtext ,
                                          `trace` varchar(255) DEFAULT NULL,
                                          `file_name` varchar(255) DEFAULT NULL,
                                          `func_name` varchar(255) DEFAULT NULL,
                                          `line_no` int(11) DEFAULT NULL,
                                          `create_time` datetime DEFAULT NULL,
                                          `input_msg` longtext ,
                                          `project` varchar(100) DEFAULT NULL,
                                          `sessionId` varchar(100) DEFAULT NULL,
                                          `version` varchar(100) DEFAULT NULL,
                                          `sence_id` varchar(100) DEFAULT NULL,
                                          `env` varchar(100) DEFAULT NULL,
                                          PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=420256 DEFAULT CHARSET=utf8