/*
typedef struct DBConnection DBConnection;
typedef struct Constant Constant;
extern "C" {
DBConnection* ddb_create_connection();
int ddb_connect(DBConnection* conn, const char* host, int port, const char* user, const char* password, const char* startup, int highAvailiablity);
void ddb_login(DBConnection* conn, const char* userId, const char* password, int enableEncryption);
Constant* ddb_run1(DBConnection* conn, const char* script, int priority, int parallelism);
Constant* ddb_run2(DBConnection* conn, const char* funcName, Constant** args, int size, int priority, int parallelism);
void ddb_upload(DBConnection* conn, const char* name, Constant* obj);
void ddb_upload2(DBConnection* conn, char** names, Constant** objs, int size);
void ddb_close(DBConnection* conn);
void ddb_initialize();
}

extern "C" {
const char* ddb_getString(Constant* val);
}
*/
/* 
#include "DolphinDB.h"

using namespace dolphindb;

#ifndef WRAPPER_H_
#define WRAPPER_H_

#ifdef __cplusplus
extern "C" {
#endif
//typedef struct DBConnection DBConnection;
//typedef struct Constant Constant;

DBConnection* DBConnection_Create();
// void *call_Person_Create();
// void call_Person_Destroy(void *);
// int call_Person_GetAge(void *);
// const char *call_Person_GetName(void *);
int DBConnection_connect(DBConnection* conn, char* host, int port, char* user,
                         char* pass);

int DBConnection_run(DBConnection* conn, char* s);

#ifdef __cplusplus
}
#endif

#endif  // WRAPPER_H_

 */