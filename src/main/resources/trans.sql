select sessionId,collect(concat('"',version,'"',':',if(left(input_msg,1) in ('{','"'),input_msg,concat('"',input_msg,'"')))) as input_msg
 from ods_logger_yewu
 where sessionId = '05d4340d-4fb0-4e7e-9b5c-f93096a17503'
  and create_time >= '2022-08-23 00:00:00'
 group by sessionId