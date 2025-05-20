// to execute this script you need firebird include/ibase.h and fbclient.dll
// build command gcc db_parser.c -I"<FIREBIRDPATH>/include" "<FIREBIRDPATH>/bin/fbclient.dll"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ibase.h" // Firebird client API
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)  // Windows не использует `mode`
#endif

#define DB_NAME ""
#define DB_USER ""
#define DB_PASSWORD ""
#define DIR_NAME "script_output"

isc_db_handle db = NULL;
isc_tr_handle trans = NULL;
ISC_STATUS status[20];

void check_isc_status(const char* msg);
void parse_tables(const char** tables_names);
void export_table(const char *table_name, int* addr_amt);
void replace_escape_symbol(char* original, char* modified);
void save_table_names(char** tables_names, int* tables_rows_amt, int tables_amt);
void create_directory_if_not_exists(const char *path);
int directory_exists(const char *path);
char** get_tables_names(int* elements_amount);

typedef struct {
    short vary_length;
    char vary_string[1];
} VARY2;

int main() {
    char dpb_buffer[128];
    char* dpb = dpb_buffer;
	
    create_directory_if_not_exists(DIR_NAME);
	
    // Build DPB (database parameter block)
    *dpb++ = isc_dpb_version1;
    *dpb++ = isc_dpb_user_name;
    *dpb++ = (char)strlen(DB_USER);
    strcpy(dpb, DB_USER);
    dpb += strlen(DB_USER);
    *dpb++ = isc_dpb_password;
    *dpb++ = (char)strlen(DB_PASSWORD);
    strcpy(dpb, DB_PASSWORD);
    dpb += strlen(DB_PASSWORD);
	
	printf("%s\n", dpb_buffer);
	printf("%d\n", (int)dpb_buffer[1]);
	printf("%d\n", (int)dpb_buffer[2]);
    // Connect to database
    isc_attach_database(status, 0, DB_NAME, &db, dpb - dpb_buffer, dpb_buffer);
    check_isc_status("attach database");
	
    int elements_amt=0;
	char** tables_names = get_tables_names(&elements_amt);
	int* saves_amt = malloc(sizeof(int)*elements_amt);
	
	for (int i=0; i<elements_amt; i++){
		printf("%s\n", tables_names[i]);
		export_table(tables_names[i], (saves_amt+i));
	}
	
	for (int* i=saves_amt; i<saves_amt+elements_amt; i++){
		printf("%d\t", *i);
	}
	printf("\n");
	save_table_names(tables_names, saves_amt, elements_amt);

    // Cleanup
	for (int i=0; i<elements_amt; i++){free(tables_names[i]);}
	free(tables_names);
	free(saves_amt);
    isc_detach_database(status, &db);
    return 0;
}


char** get_tables_names(int* elements_amount){
	// Start transaction
    isc_start_transaction(status, &trans, 1, &db, 0, NULL);
    check_isc_status("start transaction");

    // Prepare and execute SQL
    char sql[] = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0";
    isc_stmt_handle stmt = NULL;
    XSQLDA* sqlda = (XSQLDA*)malloc(XSQLDA_LENGTH(1));
    sqlda->sqln = 1;
    sqlda->version = 1;

    isc_dsql_allocate_statement(status, &db, &stmt);
    check_isc_status("allocate statement");

    isc_dsql_prepare(status, &trans, &stmt, 0, sql, 1, sqlda);
    check_isc_status("prepare statement");

    isc_dsql_execute(status, &trans, &stmt, 1, NULL);
    check_isc_status("execute statement");

    // Describe output
    isc_dsql_describe(status, &stmt, 1, sqlda);
    check_isc_status("describe output");

    sqlda->sqlvar[0].sqldata = malloc(64);
    sqlda->sqlvar[0].sqlind = malloc(sizeof(short));

    // Fetch rows
	// Saving tables names
	int tables_amount = 5;
	int iteration = 0;
	
	char** tables_names = malloc(sizeof(char*)*tables_amount);
	char** tab_n = tables_names;
	
	
    while (isc_dsql_fetch(status, &stmt, 1, sqlda) == 0) {
        char table_name[65];
        strncpy(table_name, sqlda->sqlvar[0].sqldata, 64);
        table_name[64] = '\0';
        // Remove trailing spaces
		int i=0;
        for (; i <= strlen(table_name) && table_name[i] != ' '; i++){}
		table_name[i]='\0';

		if (iteration>=tables_amount){
			char** temp = realloc(tables_names, sizeof(char*)*(iteration+2*tables_amount));
			if (temp == NULL){
				printf("Memory allocation error while extending tables_names size");
				break;
			}
			tables_names = temp;
			tab_n = tables_names;
			tab_n += iteration;
		}
		*tab_n = malloc(sizeof(char)*65);
		strcpy(*tab_n, table_name);
		tab_n++;
		iteration++;
    }
	free(sqlda->sqlvar[0].sqldata);
    free(sqlda->sqlvar[0].sqlind);
    free(sqlda);
	isc_dsql_free_statement(status, &stmt, DSQL_drop);
    isc_commit_transaction(status, &trans);
	*elements_amount = iteration;
	return tables_names;
}

void save_table_names(char** tables_names, int* tables_rows_amt, int tables_amt){
	char filename[256];
	sprintf(filename, "%s/table_names.txt", DIR_NAME);
	
	FILE* fp = fopen(filename, "w");
	if (fp == NULL){
		perror("File with table name didn't open\n");
		return;
	}
	
	for (int i=0; i<tables_amt; i++){
		fprintf(fp, "%s", tables_names[i]);
		fprintf(fp, " %d", *(tables_rows_amt+i));
		fprintf(fp, "\n");
	}
	
	fclose(fp);
}


void check_isc_status(const char* msg) {
    if (status[0] == 1 && status[1]) {
        fprintf(stderr, "Error (%s): %s\n", msg, (char*)status + 3);
        exit(1);
    }
}


int directory_exists(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0 && (st.st_mode & S_IFDIR));
}

void create_directory_if_not_exists(const char *path) {
    if (!directory_exists(path)) {
        if (mkdir(path, 0777) == 0) {
            printf("Directory created: %s\n", path);
        } else {
            perror("Directory creating error\n");
        }
    } else {
        printf("Directory already exist: %s\n", path);
    }
}


void replace_escape_symbol(char* original, char* modified){
    char* c_m=modified;
	for (char* c=original; *c!='\0'; c++, c_m++){
		if (*c=='\n' || *c=='\025' || *c=='\036' || *c=='\r\n' || *c=='\n\r'){
			strcpy(c_m, "<NL>");
			c_m += 3;
			continue;
		}
		else if (*c=='\r'){
			strcpy(c_m, "<RR>");
			c_m += 3;
			continue;
		}
		*c_m = *c;
	}
	*c_m = '\0';
}

void export_table(const char *table_name, int* addr_amt) {
	
    char sql[512], filename[256];
    FILE *out;
    isc_stmt_handle stmt = NULL;
    XSQLDA *sqlda = (XSQLDA*)malloc(XSQLDA_LENGTH(1));
    int num_cols, i;
    
    // 2) Select all data from the table
    sprintf(sql, "SELECT * FROM %s", table_name);
	
    isc_start_transaction(status, &trans, 1, &db, 0, NULL);
    check_isc_status("Failed to start transaction");
	
	printf("Prepared statment: %s\n\n", sql);
	
    // Prepare the SELECT to find column count
    sqlda = (XSQLDA*) malloc(XSQLDA_LENGTH(1));
    sqlda->sqln = 1; sqlda->version = 1;
	
	isc_dsql_allocate_statement(status, &db, &stmt);
    check_isc_status("allocate statement for data select query");
	
    isc_dsql_prepare(status, &trans, &stmt, 0, sql, 1, sqlda);
    check_isc_status("Failed to prepare data select query");
	
    num_cols = sqlda->sqld;
	printf("Table %s have %d cols\n\n", table_name, num_cols);
    // Reallocate XSQLDA if needed and describe
    
	if (sqlda->sqln < num_cols) {
        sqlda = (XSQLDA*) realloc(sqlda, XSQLDA_LENGTH(num_cols));
        sqlda->sqln = num_cols; sqlda->version = 1;
		
        isc_dsql_describe(status, &stmt, 1, sqlda);
        check_isc_status("Failed to describe select query");
		
        num_cols = sqlda->sqld;
    }
    // Allocate a single buffer for all columns in a row
    
	int total_length = 0;
    for (i = 0; i < num_cols; i++) {
        XSQLVAR *var = &sqlda->sqlvar[i];
        short dtype = var->sqltype & ~1;
        short length = var->sqllen;
        if (dtype == SQL_VARYING) length += sizeof(short);
        // Align on natural boundaries
        int align = (dtype==SQL_TEXT ? 1 :
                     (dtype==SQL_VARYING? sizeof(short) : sizeof(int)));
        total_length = FB_ALIGN(total_length, align) + length;
        total_length = FB_ALIGN(total_length, sizeof(short)) + sizeof(short);
    }
	
    char *buffer = malloc(total_length);
    // Assign sqldata and sqlind for each field
    int offset = 0;
    for (i = 0; i < num_cols; i++) {
        XSQLVAR *var = &sqlda->sqlvar[i];
        short dtype = var->sqltype & ~1;
        short length = var->sqllen;
        if (dtype == SQL_VARYING) length += sizeof(short);
        int align = (dtype==SQL_TEXT ? 1 :
                     (dtype==SQL_VARYING? sizeof(short) : sizeof(int)));
        offset = FB_ALIGN(offset, align);
        var->sqldata = buffer + offset;
        offset += length;
        offset = FB_ALIGN(offset, sizeof(short));
        var->sqlind = (short*)(buffer + offset);
        offset += sizeof(short);
    }
    // Execute the SELECT
    isc_dsql_execute(status, &trans, &stmt, 1, NULL);
    check_isc_status("Failed to execute select query");
    // Open output file "TABLENAME.txt"
    sprintf(filename, "%s/%s.txt", DIR_NAME, table_name);
    out = fopen(filename, "w");
	
    if (!out) {
        fprintf(stderr, "Cannot open file %s\n", filename);
        isc_dsql_free_statement(status, &stmt, DSQL_drop);
        free(sqlda);
        return;
    }
	
	int total_rows_amt = 0;
    // 3) Fetch each row and write COLUMN:DATA lines
    while (isc_dsql_fetch(status, &stmt, 1, sqlda) == 0) {
        for (i = 0; i < num_cols; i++) {
            XSQLVAR *var = &sqlda->sqlvar[i];

            char colname[65];
			
            int len = var->aliasname_length ? var->aliasname_length : var->sqlname_length;
            memcpy(colname, var->aliasname_length ? var->aliasname : var->sqlname, len);
            colname[len] = '\0';
            for(int k=len-1; k>=0 && colname[k]==' '; --k) colname[k] = '\0';

            // Check NULL indicator
            if ((var->sqltype & 1) && *var->sqlind < 0) {
                fprintf(out, "%s:NULL\t", colname);
                continue;
            }
            short dtype = var->sqltype & ~1;
            // Handle each data type
            switch(dtype) {
                case SQL_TEXT: {
                    // Fixed-length string
                    char text[512];
					char text_validated[1024];
                    memcpy(text, var->sqldata, var->sqllen);
                    text[var->sqllen] = '\0';
					replace_escape_symbol(text, text_validated);
					// Need to replace \n to \N in order to rellocation process issues
                    fprintf(out, "%s:%s\t", colname, text_validated);
                    break;
                }
                case SQL_VARYING: {
                    // VARCHAR: first 2 bytes length, then data
                    VARY2 *v = (VARY2*) var->sqldata;
                    int vlen = v->vary_length;
                    char *text = malloc(vlen+1);
                    memcpy(text, v->vary_string, vlen);
                    text[vlen] = '\0';
					int clean_len = vlen*2+1;
					char* text_validated = malloc(clean_len);
					replace_escape_symbol(text, text_validated);
					// Need to replace \n to \N in order to rellocation process issues
                    fprintf(out, "%s:%s\t", colname, text_validated);
                    free(text);
					free(text_validated);
                    break;
                }
                case SQL_SHORT:
                    fprintf(out, "%s:%d\t", colname, *(short*)var->sqldata);
                    break;
                case SQL_LONG:
                    fprintf(out, "%s:%d\t", colname, *(int*)var->sqldata);
                    break;
                case SQL_INT64:
                    fprintf(out, "%s:%lld\t", colname, (long long)*(ISC_INT64*)var->sqldata);
                    break;
                case SQL_FLOAT:
                    fprintf(out, "%s:%g\t", colname, *(float*)var->sqldata);
                    break;
                case SQL_DOUBLE:
                    fprintf(out, "%s:%f\t", colname, *(double*)var->sqldata);
                    break;
                case SQL_TIMESTAMP: {
                    // Decode TIMESTAMP to YYYY-MM-DD HH:MM:SS&#8203;:contentReference[oaicite:8]{index=8}
                    struct tm t;
                    isc_decode_timestamp((ISC_TIMESTAMP*)var->sqldata, &t);
                    char datestr[32];
                    sprintf(datestr, "%04d-%02d-%02d %02d:%02d:%02d",
                            t.tm_year+1900, t.tm_mon+1, t.tm_mday,
                            t.tm_hour, t.tm_min, t.tm_sec);
                    fprintf(out, "%s:%s\t", colname, datestr);
                    break;
                }
                case SQL_TYPE_DATE: {
                    struct tm t;
                    isc_decode_sql_date((ISC_DATE*)var->sqldata, &t);
                    char datestr[16];
                    sprintf(datestr, "%04d-%02d-%02d",
                            t.tm_year+1900, t.tm_mon+1, t.tm_mday);
                    fprintf(out, "%s:%s\t", colname, datestr);
                    break;
                }
                case SQL_TYPE_TIME: {
                    struct tm t;
                    isc_decode_sql_time((ISC_TIME*)var->sqldata, &t);
                    char timestr[16];
                    sprintf(timestr, "%02d:%02d:%02d",
                            t.tm_hour, t.tm_min, t.tm_sec);
                    fprintf(out, "%s:%s\t", colname, timestr);
                    break;
                }
                case SQL_BLOB: {
                    // Print BLOB ID (OID) to represent blob data
                    ISC_QUAD bid = *(ISC_QUAD*)var->sqldata;
                    fprintf(out, "%s:%08x:%08x\t", colname,
                            bid.gds_quad_high, bid.gds_quad_low);
                    break;
                }
                default:
                    fprintf(out, "%s:(unhandled type %d)\t", colname, dtype);
                    break;
            }
        }
		total_rows_amt++;
        fprintf(out, "\n");  // blank line between rows (optional)
    }
	*addr_amt = total_rows_amt;
	//*rows_amt_addr = total_rows_amt;
	printf("Table %s contains %d rows\n\n", table_name, total_rows_amt);
    // Cleanup
	
    isc_dsql_free_statement(status, &stmt, DSQL_drop);
    fclose(out);
	free(sqlda->sqlvar[0].sqldata);
    free(sqlda->sqlvar[0].sqlind);
    free(sqlda);
	free(buffer);
    // Commit the data transaction&#8203;:contentReference[oaicite:9]{index=9}
    isc_commit_transaction(status, &trans);
    check_isc_status("Failed to commit transaction (data)");
}
