import requests
import json
from datetime import datetime
import dateutil.relativedelta
import sys
import configparser

import pymssql	#	pip install pymssql

#	사용자 함수 : DB Query 수행
def udef_dbExecute(query, params):
	conn = pymssql.connect(	host		=config.get("DB", "host")\
							,port		=config.get("DB", "port")\
							,server		=config.get("DB", "server")\
							,user		=config.get("DB", "user")\
							,password	=config.get("DB", "password")\
							,database	=config.get("DB", "database")\
							)
	cursor = conn.cursor()
	cursor.execute(query, params)
	conn.commit()
	conn.close()

#	API 처리 로그 적재
def udef_setApiLog(yymm, apiType, msg, remark):
	query = "EXEC SetApiLog @pYYMM=%s, @pApiType=%s, @pErrYn='Y', @pRowCnt=0, @pMsg=%s, @pRemark=%s"
	params = (yymm, apiType, msg, remark)
	udef_dbExecute(query, params)

#	-difAmt 월 구하기
def udef_getPrevMonth(difAmt):
	now = datetime.now()
	prevDate = now + dateutil.relativedelta.relativedelta(months=-difAmt)
	return prevDate.strftime("%Y%m")

config = configparser.RawConfigParser()
config.read("globalVal.ini", encoding="UTF-8")

#	Python 실행 파라미터 Start

#	_apiType : Flight, Passenger
#	API Logging용 apiType : 파이썬 수행 시 파라미터로 전달받으며 별도의 파라미터가 없을 경우 기본값 Passenger로 처리
_apiType 			= sys.argv[1] if len(sys.argv) >=2 else "Passenger"
#	2번째 실행 파라미터 
#	정기/부정기 코드 : 정기(0), 부정기(1)
#	빈 값일 경우 전체를 대상으로 수행
_periodicity		= sys.argv[2] if len(sys.argv) >=3 else ""
#	3번째 실행 파라미터 : _apiType에 따라 내용이 다름
#	_apiType == 'Flight' 	=> pax_cargo : 여객기(Y), 화물기(N)
#	_apiType == 'Passenger' => passenger_type : 유임승객(1), 무임승객(2), 환승승객(3)
#	빈 값일 경우 전체를 대상으로 수행
_argv3				= sys.argv[3] if len(sys.argv) >=4 else ""

#	Python 실행 파라미터 End

_keyDec				= config.get("API", "keyDec")
_successResultCode 	= config.get("API", "successResultCode")
_noDataMsg			= config.get("COMMON", "noDataMsg")

#	수행대상 년월 : 현재월 기준
_yymm 	= datetime.today().strftime("%Y%m")

api_url = config.get("API", "url")
if _apiType == "Passenger":
	api_url	+= config.get("API", "methodPassenger")
	api_params ={
		'serviceKey' 		: _keyDec
		,'from_month' 		: _yymm
		,'to_month' 		: _yymm
		,'periodicity' 		: _periodicity
		,'passenger_type' 	: _argv3
		,'type' 			: 'json' 
	}
else:	# _apiType == "Flight"
	api_url	+= config.get("API", "methodFlight")
	api_params ={
		'serviceKey' 	: _keyDec
		,'from_month' 	: _yymm
		,'to_month' 	: _yymm
		,'periodicity' 	: _periodicity
		,'pax_cargo' 	: _argv3
		,'type' 		: 'json' 
	}

#	API resultCode가 비정상 이거나 해당 월 데이터가 없을 경우 최대 3회까지만 재시도
#		Case1. API 호출 실패 : 해당 월 재시도
# 		Case2. 해당 월 데이터 미존재 : 이전 월 재시도
# 			※ 수행 시점 대비 전전월 데이터는 필수적으로 존재한다는 업무 정의를 가정
i = 0
noDataCnt = 0
for i in range(0, 3):
	response = requests.get(api_url, params=api_params)

	# 공공데이터포털에서 출력되는 오류메세지는 type 설정과 무관하게 XML로만 출력, XML 파싱 요망
	if response.text[0] == "<":
		from lxml import etree	# pip install lxml
		xml_result = etree.XML(response.text)
		msg = xml_result.xpath("//OpenAPI_ServiceResponse//cmmMsgHeader//returnAuthMsg")[0].text
		code = xml_result.xpath("//OpenAPI_ServiceResponse//cmmMsgHeader//returnReasonCode")[0].text
		udef_setApiLog(_yymm, _apiType, msg, code)
		break

	#	Key 오류 등으로 인한 실패 시 type에 JSON으로 설정과 무관하게 XML 형태로 리턴되어 별도의 예외처리, 반복없이 종료
	json_result		= json.loads(response.text)["response"]
	api_resultCode 	= json_result["header"]["resultCode"]
	api_resultMsg 	= json_result["header"]["resultMsg"]

	#	API 수행이 실패했을 경우 재시도
	#print("api_resultCode :",api_resultCode)
	if api_resultCode != _successResultCode:
		udef_setApiLog(_yymm, _apiType, api_resultMsg, "")
		continue

	#	API 수행은 성공했으나 결과값이 없을 경우 이전 월로 재시도
	#print("len :",len(json_result["body"]["items"]))
	if len(json_result["body"]["items"]) == 0:
		udef_setApiLog(_yymm, _apiType, _noDataMsg, "")
		noDataCnt += 1
		_yymm = udef_getPrevMonth(noDataCnt)
		api_params["from_month"] 	= _yymm
		api_params["to_month"] 		= _yymm
		continue

	#	정상 수행되는 경우 SP 내부적으로 API 로그 적재함
	if _apiType == "Passenger":
		query = "EXEC GetApiPassenger @pYYMM=%s, @pJSON=%s, @pApiType=%s, @pPeriodicity=%s, @pPassenger_type=%s"
	else:	# _apiType == "Flight"
		query = "EXEC GetApiFlight @pYYMM=%s, @pJSON=%s, @pApiType=%s, @pPeriodicity=%s, @pPax_cargo=%s"

	params = (_yymm, str(json_result["body"]["items"]).replace(r"'", r'"'), _apiType, _periodicity, _argv3)
	udef_dbExecute(query, params)
	break
	#	임시로 수행 성공했을 때 이전월 호출로직 추가 Start
	# noDataCnt += 1
	# _yymm = udef_getPrevMonth(noDataCnt)
	# api_params["from_month"] 	= _yymm
	# api_params["to_month"] 		= _yymm
	#	임시로 수행 성공했을 때 이전월 호출로직 추가 End
	