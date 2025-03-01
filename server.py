from fastapi import FastAPI, Header
from getData import get_file_name, get_query, get_query_all, convert_record_to_json
import globals
from s3 import get_data_from_s3, isKeyExist, save_to_s3, getDfFromS3
from enum_data import FileNameKey, ReturnDataType, QABody 
import pandas as pd
import json

app = FastAPI()

@app.get("/api/q1/{login_id}")
async def read_root(login_id: str, lob: str = Header(None)):  # ✅ Correct way to get header
    print("qa")
    if globals.test:
        lob = "perfettisfai"  
    elif not lob:  
        return {"error": "lob header is missing"}
    
    file_name = get_file_name(lob, FileNameKey.OPPORTUNITY_OUTLETS)
    query = get_query(login_id )
    
    print(file_name, query)
    
    response = get_data_from_s3(file_name=file_name, query=query, returnType=ReturnDataType.OBJECT)

    print(response)
    
    return response


@app.get("/api/q1/questions/")
async def read_questions( lob: str = Header(None)):  # ✅ Correct way to get header
    print("qa")
    if globals.test:
        lob = "perfettisfai"  
    elif not lob:  
        return {"error": "lob header is missing"}
    
    file_name = get_file_name(lob, FileNameKey.QUESTIONS)
    query = get_query_all( )
    
    print(file_name, query)
    
    response = get_data_from_s3(file_name=file_name, query=query, returnType=ReturnDataType.ARRAY)

    print(response)
    
    return response

@app.post("/api/q1/answers/")
async def post_answers(body:QABody, lob: str = Header(None)):
    if globals.test:
        lob = "perfettisfai"  
    elif not lob:  
        return {"error": "lob header is missing"}
    file_name = FileNameKey.ANSWERS.value.format(lob)

    payload_json = json.dumps([entry.dict() for entry in body.payload], ensure_ascii=False)
    # print(body)
    if(isKeyExist(file_name)):
        
        df = getDfFromS3(file_name, 0, 0, 0, False)
        existing_row_index = df[(df["loginid"] == body.loginid) & (df["outletcode"] == body.outletcode)].index

        if not existing_row_index.empty:
            df.at[existing_row_index[0], "payload"] = payload_json
        else:
            df = df._append({
                "loginid": body.loginid,
                "outletcode": body.outletcode,
                "payload": payload_json
            }, ignore_index=True)
    else:
        
        df = pd.DataFrame([{
            "loginid": body.loginid,
            "outletcode": body.outletcode,
            "payload": payload_json  
        }])


    save_to_s3(file_name, df)
    
    return {"successfully pushed!!"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
