#########################################################
# Simplified PyPy SQLite wrapper compatible with APSW
#########################################################

from collections import OrderedDict
from cffi import FFI
from functools import wraps
import weakref
from codecs import utf_8_decode
from threading import _get_ident as thread_get_ident
from types import NoneType
from threading import _get_ident as _thread_get_ident
import sys
import os

if hasattr(sys, 'pypy_version_info'):
    from __pypy__ import newlist_hint
else:
    newlist_hint = lambda size: []

ffi = FFI()
sqlite = ffi.dlopen("sqlite3")

# pysqlite version information
version = "2.6.0"

# pysqlite constants
PARSE_COLNAMES = 1
PARSE_DECLTYPES = 2

SQLITE_OPEN_READWRITE = 2
SQLITE_OPEN_CREATE = 4
SQLITE_DENY = 1





###########################################
# BEGIN Wrapped SQLite C API and constants
###########################################

ffi.cdef("""
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef void (*sqlite3_destructor_type)(void*);
typedef struct sqlite3_context sqlite3_context;
typedef struct Mem sqlite3_value;
const char *sqlite3_errmsg(sqlite3*);
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs);
int sqlite3_close(sqlite3*);
int sqlite3_busy_timeout(sqlite3 *, int ms);
int sqlite3_total_changes(sqlite3*);
int sqlite3_prepare(sqlite3 *db,const char *zSql,int nByte,sqlite3_stmt **ppStmt,const char **pzTail);
int sqlite3_prepare_v2(sqlite3 *db,const char *zSql,int nByte,sqlite3_stmt **ppStmt,const char **pzTail);
int sqlite3_step(sqlite3_stmt*);
int sqlite3_reset(sqlite3_stmt *pStmt);
int sqlite3_finalize(sqlite3_stmt *pStmt);
int sqlite3_column_count(sqlite3_stmt *pStmt);
const char *sqlite3_column_name(sqlite3_stmt*, int N);
int sqlite3_column_type(sqlite3_stmt*, int iCol);
int sqlite3_extended_result_codes(sqlite3*, int onoff);
int64_t sqlite3_column_int64(sqlite3_stmt*, int iCol);
double sqlite3_column_double(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
const char *sqlite3_column_text(sqlite3_stmt*, int iCol);
const char *sqlite3_column_text16(sqlite3_stmt*, int iCol);
const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
const char *sqlite3_column_decltype(sqlite3_stmt*,int);
int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
int sqlite3_bind_double(sqlite3_stmt*, int, double);
int sqlite3_bind_int(sqlite3_stmt*, int, int);

int sqlite3_bind_null(sqlite3_stmt*, int);
int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int, void(*)(void*));
int sqlite3_bind_value(sqlite3_stmt*, int, const sqlite3_value*);
int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int n);
int sqlite3_clear_bindings(sqlite3_stmt*);
int sqlite3_value_type(sqlite3_value*);
int64_t sqlite3_value_int64(sqlite3_value*);
int sqlite3_value_int(sqlite3_value*);
double sqlite3_value_double(sqlite3_value*);
int sqlite3_bind_parameter_count(sqlite3_stmt*);
int sqlite3_value_bytes(sqlite3_value*);
const unsigned char *sqlite3_value_text(sqlite3_value*); //Not used.
const void *sqlite3_value_blob(sqlite3_value*);
int sqlite3_changes(sqlite3*);
void sqlite3_result_error(sqlite3_context*, const char*, int);
void sqlite3_result_error_code(sqlite3_context*, int);
void sqlite3_result_int64(sqlite3_context*, int64_t);
void sqlite3_result_int(sqlite3_context*, int);
void sqlite3_result_double(sqlite3_context*, double);
void sqlite3_result_null(sqlite3_context*);
void sqlite3_result_text(sqlite3_context*, const char*, int, void(*)(void*));
void sqlite3_result_text16(sqlite3_context*, const char*, int, void(*)(void*));
void sqlite3_result_blob(sqlite3_context*, const void*, int, void(*)(void*));
void *sqlite3_aggregate_context(sqlite3_context*, int nBytes);
int sqlite3_complete(const char *sql);
int sqlite3_errcode(sqlite3 *db);
const char *sqlite3_libversion(void);
int sqlite3_open(char *, sqlite3 **ppDb);
typedef void (*ljsqlite3_cbstep)(sqlite3_context*,int,sqlite3_value**);
typedef void (*ljsqlite3_cbfinal)(sqlite3_context*);

int sqlite3_create_function(
  sqlite3 *db,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
int sqlite3_get_autocommit(sqlite3*);
int sqlite3_create_collation(
          void*,
          const char *,
          int eTextRep,
          void *pArg,
          int(*)(void *, int, void*, int, void*)
    );
int sqlite3_process_handler(void *, int, int(*)(void *), void *);

int sqlite3_set_authorizer(
    sqlite3*,
    int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
    void *pUserData
);

int64_t sqlite3_last_insert_rowid(sqlite3*);
int sqlite3_bind_int64(sqlite3_stmt*, int, int64_t);
/*
typedef struct {
  sqlite3* _ptr;
  bool     _closed;
} ljsqlite3_conn;

typedef struct {
  sqlite3_stmt* _ptr;
  bool          _closed;
  sqlite3*      _conn;
  int32_t       _code;
} ljsqlite3_stmt;

*/

struct sqlite3_index_info {
  /* Inputs */
  int nConstraint;           /* Number of entries in aConstraint */
  struct sqlite3_index_constraint {
     int iColumn;              /* Column on left-hand side of constraint */
     unsigned char op;         /* Constraint operator */
     unsigned char usable;     /* True if this constraint is usable */
     int iTermOffset;          /* Used internally - xBestIndex should ignore */
  } *aConstraint;            /* Table of WHERE clause constraints */
  int nOrderBy;              /* Number of terms in the ORDER BY clause */
  struct sqlite3_index_orderby {
     int iColumn;              /* Column number */
     unsigned char desc;       /* True for DESC.  False for ASC. */
  } *aOrderBy;               /* The ORDER BY clause */
  /* Outputs */
  struct sqlite3_index_constraint_usage {
    int argvIndex;           /* if >0, constraint is part of argv to xFilter */
    unsigned char omit;      /* Do not code a test for this constraint */
  } *aConstraintUsage;
  int idxNum;                /* Number used to identify the index */
  char *idxStr;              /* String, possibly obtained from sqlite3_malloc */
  int needToFreeIdxStr;      /* Free idxStr using sqlite3_free() if true */
  int orderByConsumed;       /* True if output is already ordered */
  double estimatedCost;           /* Estimated cost of using this index */
  /* Fields below are only available in SQLite 3.8.2 and later */
  long long estimatedRows;    /* Estimated number of rows returned */
};

typedef struct sqlite3_vtab sqlite3_vtab;
typedef struct sqlite3_index_info sqlite3_index_info;
typedef struct sqlite3_vtab_cursor sqlite3_vtab_cursor;
typedef struct sqlite3_module sqlite3_module;
typedef long long int sqlite_int64;
typedef unsigned long long int sqlite_uint64;
typedef sqlite_int64 sqlite3_int64;
typedef sqlite_uint64 sqlite3_uint64;

struct sqlite3_vtab {
  const sqlite3_module *pModule;  /* The module for this virtual table */
  int nRef;                       /* Used internally */
  char *zErrMsg;                  /* Error message from sqlite3_mprintf() */
  /* Virtual table implementations will typically add additional fields */
};
struct sqlite3_vtab_cursor {
  sqlite3_vtab *pVtab;      /* Virtual table of this cursor */
  /* Virtual table implementations will typically add additional fields */
int n;
};
struct sqlite3_module {
  int iVersion;
  int (*xCreate)(sqlite3*, void *pAux,
               int argc, const char *const*argv,
               sqlite3_vtab **ppVTab, char**);
  int (*xConnect)(sqlite3*, void *pAux,
               int argc, const char *const*argv,
               sqlite3_vtab **ppVTab, char**);
  int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*);
  int (*xDisconnect)(sqlite3_vtab *pVTab);
  int (*xDestroy)(sqlite3_vtab *pVTab);
  int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
  int (*xClose)(sqlite3_vtab_cursor*);
  int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                int argc, sqlite3_value **argv);
  int (*xNext)(sqlite3_vtab_cursor*);
  int (*xEof)(sqlite3_vtab_cursor*);
  int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context*, int);
  int (*xRowid)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid);
  int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *);
  int (*xBegin)(sqlite3_vtab *pVTab);
  int (*xSync)(sqlite3_vtab *pVTab);
  int (*xCommit)(sqlite3_vtab *pVTab);
  int (*xRollback)(sqlite3_vtab *pVTab);
  int (*xFindFunction)(sqlite3_vtab *pVtab, int nArg, const char *zName,
                       void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
                       void **ppArg);
  int (*xRename)(sqlite3_vtab *pVtab, const char *zNew);
  /* The methods above are in version 1 of the sqlite_module object. Those
  ** below are for version 2 and greater. */
  int (*xSavepoint)(sqlite3_vtab *pVTab, int);
  int (*xRelease)(sqlite3_vtab *pVTab, int);
  int (*xRollbackTo)(sqlite3_vtab *pVTab, int);
};
int sqlite3_declare_vtab(sqlite3 *db, const char *zCreateTable);
int sqlite3_create_module(
  sqlite3 *db,               /* SQLite connection to register module with */
  const char *zName,         /* Name of the module */
  const sqlite3_module *p,   /* Methods for the module */
  void *pClientData         /* Client data for xCreate/xConnect */
  );
  void *sqlite3_malloc(int);
void *sqlite3_realloc(void*, int);
void sqlite3_free(void*);
char *sqlite3_mprintf(const char*,...);
"""
)

_sqresult_text = sqlite.sqlite3_result_text
_sqresult_text16 = sqlite.sqlite3_result_text16
SQLITE_TRANSIENT = ffi.cast("const void *", -1)
SQLITE_STATIC = ffi.cast("const void *", 0)
FTS3_FULLSCAN_SEARCH = 0
SQLITE_OK = 0
SQLITE_ERROR = 1
SQLITE_INTERNAL = 2
SQLITE_PERM = 3
SQLITE_ABORT = 4
SQLITE_BUSY = 5
SQLITE_LOCKED = 6
SQLITE_NOMEM = 7
SQLITE_READONLY = 8
SQLITE_INTERRUPT = 9
SQLITE_IOERR = 10
SQLITE_CORRUPT = 11
SQLITE_NOTFOUND = 12
SQLITE_FULL = 13
SQLITE_CANTOPEN = 14
SQLITE_PROTOCOL = 15
SQLITE_EMPTY = 16
SQLITE_SCHEMA = 17
SQLITE_TOOBIG = 18
SQLITE_CONSTRAINT = 19
SQLITE_MISMATCH = 20
SQLITE_MISUSE = 21
SQLITE_NOLFS = 22
SQLITE_AUTH = 23
SQLITE_FORMAT = 24
SQLITE_RANGE = 25
SQLITE_NOTADB = 26
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_INTEGER = 1
SQLITE_FLOAT = 2
SQLITE_BLOB = 4
SQLITE_NULL = 5
SQLITE_TEXT = 3
SQLITE3_TEXT = 3


SQLITE_INDEX_CONSTRAINT_MATCH = 64
SQLITE_INDEX_CONSTRAINT_EQ = 2
SQLITE_INDEX_CONSTRAINT_GE = 32
SQLITE_INDEX_CONSTRAINT_GT = 4,
SQLITE_INDEX_CONSTRAINT_LE = 8,
SQLITE_INDEX_CONSTRAINT_LT = 16,

SQLITE_UTF8 = 1

SQLITE_DENY     = 1
SQLITE_IGNORE   = 2

mapping_authorizer_function = {}
mapping_authorizer_function[1] = "SQLITE_CREATE_INDEX"
mapping_authorizer_function[2] = "SQLITE_CREATE_TABLE"
mapping_authorizer_function[3] = "SQLITE_CREATE_TEMP_INDEX"
mapping_authorizer_function[4] = "SQLITE_CREATE_TEMP_TABLE"
mapping_authorizer_function[5] = "SQLITE_CREATE_TEMP_TRIGGER"
mapping_authorizer_function[6] = "SQLITE_CREATE_TEMP_VIEW"
mapping_authorizer_function[7] = "SQLITE_CREATE_TRIGGER"
mapping_authorizer_function[8] = "SQLITE_CREATE_VIEW"
mapping_authorizer_function[9] = "SQLITE_DELETE"
mapping_authorizer_function[10] = "SQLITE_DROP_INDEX"
mapping_authorizer_function[11] = "SQLITE_DROP_TABLE"
mapping_authorizer_function[12] = "SQLITE_DROP_TEMP_INDEX"
mapping_authorizer_function[13] = "SQLITE_DROP_TEMP_TABLE"
mapping_authorizer_function[14] = "SQLITE_DROP_TEMP_TRIGGER"
mapping_authorizer_function[15] = "SQLITE_DROP_TEMP_VIEW"
mapping_authorizer_function[16] = "SQLITE_DROP_TRIGGER"
mapping_authorizer_function[17] = "SQLITE_DROP_VIEW"
mapping_authorizer_function[18] = "SQLITE_INSERT"
mapping_authorizer_function[19] = "SQLITE_PRAGMA"
mapping_authorizer_function[20] = "SQLITE_READ"
mapping_authorizer_function[21] = "SQLITE_SELECT"
mapping_authorizer_function[22] = "SQLITE_TRANSACTION"
mapping_authorizer_function[23] = "SQLITE_UPDATE"
mapping_authorizer_function[24] = "SQLITE_ATTACH"
mapping_authorizer_function[25] = "SQLITE_DETACH"
mapping_authorizer_function[26] = "SQLITE_ALTER_TABLE"
mapping_authorizer_function[27] = "SQLITE_REINDEX"
mapping_authorizer_function[28] = "SQLITE_ANALYZE"
mapping_authorizer_function[29] = "SQLITE_CREATE_VTABLE"
mapping_authorizer_function[30] = "SQLITE_DROP_VTABLE"
mapping_authorizer_function[31] = "SQLITE_FUNCTION"


SQLITE_CREATE_INDEX             = 1
SQLITE_CREATE_TABLE             = 2
SQLITE_CREATE_TEMP_INDEX        = 3
SQLITE_CREATE_TEMP_TABLE        = 4
SQLITE_CREATE_TEMP_TRIGGER      = 5
SQLITE_CREATE_TEMP_VIEW         = 6
SQLITE_CREATE_TRIGGER           = 7
SQLITE_CREATE_VIEW              = 8
SQLITE_DELETE                   = 9
SQLITE_DROP_INDEX               = 10
SQLITE_DROP_TABLE               = 11
SQLITE_DROP_TEMP_INDEX          = 12
SQLITE_DROP_TEMP_TABLE          = 13
SQLITE_DROP_TEMP_TRIGGER        = 14
SQLITE_DROP_TEMP_VIEW           = 15
SQLITE_DROP_TRIGGER             = 16
SQLITE_DROP_VIEW                = 17
SQLITE_INSERT                   = 18
SQLITE_PRAGMA                   = 19
SQLITE_READ                     = 20
SQLITE_SELECT                   = 21
SQLITE_TRANSACTION              = 22
SQLITE_UPDATE                   = 23
SQLITE_ATTACH                   = 24
SQLITE_DETACH                   = 25
SQLITE_ALTER_TABLE              = 26
SQLITE_REINDEX                  = 27
SQLITE_ANALYZE                  = 28
SQLITE_CREATE_VTABLE            = 29
SQLITE_DROP_VTABLE              = 30
SQLITE_FUNCTION                 = 31

# SQLite C API
HAS_LOAD_EXTENSION = hasattr(sqlite, "sqlite3_enable_load_extension")
if HAS_LOAD_EXTENSION:
    ffi.cdef("""int sqlite3_enable_load_extension(sqlite3 *db, int onoff);""")

##########################################
# END Wrapped SQLite C API and constants
##########################################

# SQLite version information
sqlite_version = sqlite.sqlite3_libversion()

_tcache = None
_tfun = None

class ExecutionCompleteError(StandardError):
    pass

class Error(StandardError):
    pass

class SQLError(StandardError):
    pass
    #def __init__(self, exc):
    #    if not msg.startswith(type(exc)):
    #        self.message = type(exc)+str(exc)
    #    else:
    #        self.message = msg
    #def __str__(self):
    #    return self.message
    #def __unicode__(self):
    #    return self.message

class Warning(StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class AbortError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

class ConstraintError(DatabaseError):
    pass

def complete(*args):
    return sqlite.sqlite3_complete(args[0].encode('utf-8'))

def apswversion():
    return "MSPW ver 0.01"

def sqlitelibversion():
    return ffi.string(sqlite.sqlite3_libversion())

def connect(database, **kwargs):
    factory = kwargs.get("factory", Connection)
    return factory(str(database), **kwargs)

class StatementCache(object):
    def __init__(self, connection, maxcount):
        self.connection = connection
        self.maxcount = maxcount
        self.cache = OrderedDict()

    def get(self, sql):
        try:
            stat = self.cache[sql]
        except KeyError:
            stat = Statement(self.connection, sql)
            self.cache[sql] = stat
            if len(self.cache) > self.maxcount:
                self.cache.popitem(0)

        if stat.in_use:
            stat = Statement(self.connection, sql)

        return stat, stat.next_char

class Connection(object):
    def __init__(self, database, timeout=None, detect_types=0, isolation_level="",
                 check_same_thread=True, factory=None, cached_statements=100):

        db_p = ffi.new("sqlite3 **")
        if sqlite.sqlite3_open(str(database), db_p) != SQLITE_OK:
            raise SQLError("Could not open database")

        if database != '':
            self.filename = os.path.abspath(database)
        else:
            self.filename = ''

        self._db = db_p[0]
        sqlite.sqlite3_extended_result_codes(self._db, 1)
        if timeout is not None:
            timeout = int(timeout * 1000) # pysqlite2 uses timeout in seconds
            sqlite.sqlite3_busy_timeout(self._db, timeout)

        self.closed = False
        self.statements = []
        self.statement_counter = 0
        self._isolation_level = isolation_level
        self.detect_types = detect_types
        self.statement_cache = StatementCache(self, cached_statements)
        self.cursors = []
        self.__func_cache = {}
        self.authorizer = 0
        self.Error = Error
        self.Warning = Warning
        self.InterfaceError = InterfaceError
        self.DatabaseError = DatabaseError
        self.InternalError = InternalError
        self.SQLError = SQLError
        self.OperationalError = OperationalError
        self.ProgrammingError = ProgrammingError
        self.IntegrityError = IntegrityError
        self.DataError = DataError
        self.NotSupportedError = NotSupportedError
        self.func_cache = {}
        self._aggregates = {}
        self._vtdatasource = {}
        self._vtmodules = {}
        self._vttables = {}
        self._vtcursors = []
        self._vtcursorinstances = tuple()
        self._vtcursorcolumn = []
        self._vtcursoreof = tuple()
        self._vtcursortables = []
        self.aggregate_instances = {}
        self._collations = {}
        if check_same_thread:
            self.__thread_ident = _thread_get_ident()
        self.exectracefun = None

    def _check_closed(self):
        if not self.__initialized:
            raise ProgrammingError("Base Connection.__init__ not called.")
        if not self._db:
            raise ProgrammingError("Cannot operate on a closed database.")

    def _check_thread(self):
        try:
            if self.__thread_ident == _thread_get_ident():
                return
        except AttributeError:
            pass
        else:
            raise ProgrammingError(
                "SQLite objects created in a thread can only be used in that "
                "same thread. The object was created in thread id %d and this "
                "is thread id %d", self.__thread_ident, _thread_get_ident())

    def _check_thread_wrap(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self._check_thread()
            return func(self, *args, **kwargs)
        return wrapper

    def _check_closed_wrap(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self._check_closed()
            return func(self, *args, **kwargs)
        return wrapper

    def _get_exception(self, error_code=None):
        if error_code is None:
            error_code = sqlite.sqlite3_errcode(self._db)
        error_message = ffi.string(sqlite.sqlite3_errmsg(self._db))

        if error_code == SQLITE_OK:
            raise ValueError("error signalled but got SQLITE_OK")
        elif error_code in (SQLITE_INTERNAL, SQLITE_NOTFOUND):
            exc = InternalError
        elif error_code == SQLITE_NOMEM:
            exc = MemoryError
        elif error_code == SQLITE_INTERRUPT:
            return KeyboardInterrupt
        elif error_code in ( SQLITE_ERROR, SQLITE_PERM, SQLITE_ABORT, SQLITE_BUSY, SQLITE_LOCKED,
            SQLITE_READONLY,  SQLITE_IOERR, SQLITE_FULL, SQLITE_CANTOPEN,
            SQLITE_PROTOCOL, SQLITE_EMPTY, SQLITE_SCHEMA):
            exc = SQLError
        elif error_code == SQLITE_CORRUPT:
            exc = DatabaseError
        elif error_code == SQLITE_TOOBIG:
            exc = DataError
        elif error_code in (SQLITE_CONSTRAINT, SQLITE_MISMATCH):
            exc = IntegrityError
        elif error_code == SQLITE_MISUSE:
            exc = ProgrammingError
        else:
            exc = DatabaseError
        #print exc
        if "Error:" not in error_message:
            error_message = exc.__name__ + ": " + error_message

        exc = exc(error_message)
        exc.error_code = error_code
        return exc

    def _remember_statement(self, statement):
        self.statements.append(weakref.ref(statement))
        self.statement_counter += 1

        if self.statement_counter % 100 == 0:
            self.statements = [ref for ref in self.statements if ref() is not None]

    def _reset_cursors(self):
        for cursor_ref in self.cursors:
            cursor = cursor_ref()
            if cursor:
                cursor.reset = True

    def setexectrace(self, f):
        self.exectracefun = f

    def cursor(self, factory=None):
        if factory is None:
            factory = Cursor
        cur = factory(self)
        return cur

    def executemany(self, *args):
        cur = Cursor(self)
        return cur.executemany(*args)

    def execute(self, *args):
        cur = Cursor(self)
        return cur.execute(*args)

    def executescript(self, *args):
        cur = Cursor(self)
        return cur.executescript(*args)

    def __call__(self, sql):
        if not isinstance(sql, (str, unicode)):
            raise Warning("SQL is of wrong type. Must be string or unicode.")
        statement = self.statement_cache.get(sql)
        return statement

    def _get_isolation_level(self):
        return self._isolation_level
    def _set_isolation_level(self, val):
        if val is None:
            self.commit()
        if isinstance(val, unicode):
            val = str(val)
        self._isolation_level = val
    isolation_level = property(_get_isolation_level, _set_isolation_level)

    def _begin(self):
        if self._isolation_level is None:
            return
        if sqlite.sqlite3_get_autocommit(self._db):

                sql = "BEGIN " + self._isolation_level
                statement_p = ffi.new("sqlite3_stmt **")
                next_char = ffi.new("char *")

                ret = sqlite.sqlite3_prepare_v2(self._db, sql, -1, statement_p, next_char)

                statement = statement_p[0]

                if ret != SQLITE_OK:
                    raise self._get_exception(ret)
                ret = sqlite.sqlite3_step(statement)
                if ret != SQLITE_DONE:
                    raise self._get_exception(ret)

                sqlite.sqlite3_finalize(statement)

    def commit(self):
        if sqlite.sqlite3_get_autocommit(self._db):
            return

        for statement in self.statements:
            obj = statement()
            if obj is not None:
                obj.reset()
        try:
            sql = "COMMIT"
            statement_p = ffi.new("sqlite3_stmt **")
            next_char = ffi.new("char *")
            ret = sqlite.sqlite3_prepare_v2(self._db, sql, -1, statement_p, next_char)
            statement = statement_p[0]
            if ret != SQLITE_OK:
                raise self._get_exception(ret)
            ret = sqlite.sqlite3_step(statement)
            if ret != SQLITE_DONE:
                raise self._get_exception(ret)
        finally:
            sqlite.sqlite3_finalize(statement)

    def rollback(self):
        if sqlite.sqlite3_get_autocommit(self._db):
            return

        for statement in self.statements:
            obj = statement()
            if obj is not None:
                obj.reset()

        try:
            sql = "ROLLBACK"
            statement_p = ffi.new("char **")
            next_char = ffi.new("char *")
            ret = sqlite.sqlite3_prepare_v2(self._db, sql, -1, statement_p, next_char)
            statement = statement_p[0]
            if ret != SQLITE_OK:
                raise self._get_exception(ret)
            ret = sqlite.sqlite3_step(statement)
            if ret != SQLITE_DONE:
                raise self._get_exception(ret)
        finally:
            sqlite.sqlite3_finalize(statement)
            self._reset_cursors()

    def _check_closed(self):
        return
        if getattr(self, 'closed', True):
            raise ProgrammingError("Cannot operate on a closed database.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None and exc_value is None and exc_tb is None:
            self.commit()
        else:
            self.rollback()

    def _get_total_changes(self):
        return sqlite.sqlite3_total_changes(self._db)
    total_changes = property(_get_total_changes)

    def close(self):
        if self.closed:
            return
        for statement in self.statements:
            obj = statement()
            if obj is not None:
                obj.finalize()

        self.closed = True
        ret = sqlite.sqlite3_close(self._db)
        self._reset_cursors()
        if ret != SQLITE_OK:
            raise self._get_exception(ret)

    def create_collation(self, name, callback):
        name = name.upper()
        if not name.replace('_', '').isalnum():
            raise ProgrammingError("invalid character in collation name")

        if callback is None:
            del self._collations[name]
            c_collation_callback = cast(None, COLLATION)
        else:
            if not callable(callback):
                raise TypeError("parameter must be callable")

            def collation_callback(context, len1, str1, len2, str2):
                text1 = ffi.string(str1)
                text2 = ffi.string(str2)

                return callback(text1, text2)

            c_collation_callback = COLLATION(collation_callback)
            self._collations[name] = c_collation_callback

        ret = sqlite.sqlite3_create_collation(self._db, name,
                                              SQLITE_UTF8,
                                              None,
                                              c_collation_callback)
        if ret != SQLITE_OK:
            raise self._get_exception(ret)

    def set_progress_handler(self, callable, nsteps):
        if callable is None:
            c_progress_handler = cast(None, PROGRESS)
        else:
            try:
                c_progress_handler, _ = self.func_cache[callable]
            except KeyError:
                def progress_handler(userdata):
                    try:
                        ret = callable()
                        return bool(ret)
                    except Exception:
                        # abort query if error occurred
                        return 1
                c_progress_handler = PROGRESS(progress_handler)

                self.func_cache[callable] = c_progress_handler, progress_handler
        ret = sqlite.sqlite3_progress_handler(self._db, nsteps,
                                              c_progress_handler,
                                              None)
        if ret != SQLITE_OK:
            raise self._get_exception(ret)

    @_check_thread_wrap
    @_check_closed_wrap
    def setauthorizer(self, callback):
        if callback != None:
            self.authorizer = callback
            try:
                authorizer = self.__func_cache[callback]
            except KeyError:
                @ffi.callback("int(void*, int, const char*, const char*, "
                               "const char*, const char*)")
                def authorizer(userdata, action, arg1, arg2, dbname, source):
                    try:
                        arg1 = unicode(ffi.string(arg1))
                    except:
                        arg1 = None
                    try:
                        arg2 = unicode(ffi.string(arg2))
                    except:
                        arg2 = None
                    try:
                        dbname = unicode(ffi.string(dbname))
                    except:
                        dbname = None
                    try:
                        source = unicode(ffi.string(source))
                    except:
                        source = None

                    try:
                        ret = callback(action, arg1, arg2, dbname, source)
                        assert isinstance(ret, int)
                        # try to detect cases in which cffi would swallow
                        # OverflowError when casting the return value
                        assert int(ffi.cast('int', ret)) == ret
                        return ret
                    except Exception:
                        return SQLITE_DENY
                self.__func_cache[callback] = authorizer
            ret = sqlite.sqlite3_set_authorizer(self._db, authorizer, ffi.NULL)
        if callback == None:
            ret = sqlite.sqlite3_set_authorizer(self._db, ffi.NULL, ffi.NULL)
        if ret != SQLITE_OK:
            raise self._get_exception(ret)

    def createscalarfunction(self, name, callback, num_args=-1):
        try:
            c_closure, _ = self.func_cache[callback]
        except KeyError:
            def closure(context, nargs, params):
                # try:
                #     _python_to_sqlite(context, callback(*[self._sqlite_to_python_value[sqlite.sqlite3_value_type(params[ci])](params[ci]) for ci in xrange(nargs)]))

                # try:
                #     _python_to_sqlite(context, callback(*[_sqlite_to_python(params[ci]) for ci in xrange(nargs)]))

                if nargs == 0:
                    t = sqlite.sqlite3_value_type(params[0])
                    if t == 1:
                        inparams = sqlite.sqlite3_value_int64(params[0])
                    elif t == 2:
                        inparams = sqlite.sqlite3_value_double(params[0])
                    elif t == 3:
                        inparams = utf_8_decode(ffi.string(sqlite.sqlite3_value_text(params[0])))[0]
                    elif t == 4:
                        inparams = buffer(ffi.buffer(sqlite.sqlite3_value_blob(params[0]), sqlite.sqlite3_value_bytes(params[0]))[:])
                    else:
                        inparams = None

                    try:
                        _python_to_sqlite(context, callback(inparams))
                    except KeyboardInterrupt:
                        sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT)
                        return
                    except Exception, e:
                        msg = "user-defined function raised exception: "+str(e)
                        sqlite.sqlite3_result_error(context, msg, len(msg))
                    return

                inparams = newlist_hint(nargs)
                ci = 0
                while ci < nargs:
                    # inparams.append(self._sqlite_to_python_value[sqlite.sqlite3_value_type(params[ci])](params[ci]))
                    # inparams.append(_sqlite_to_python(params[ci]))
                    t = sqlite.sqlite3_value_type(params[ci])
                    if t == 1:
                        inparams.append(sqlite.sqlite3_value_int64(params[ci]))
                    elif t == 2:
                        inparams.append(sqlite.sqlite3_value_double(params[ci]))
                    elif t == 3:
                        inparams.append(utf_8_decode(ffi.string(sqlite.sqlite3_value_text(params[ci])))[0])
                    elif t == 4:
                        inparams.append(buffer(ffi.buffer(sqlite.sqlite3_value_blob(params[ci]), sqlite.sqlite3_value_bytes(params[ci]))[:]))
                    else:
                        inparams.append(None)
                    ci += 1

                try:
                    _python_to_sqlite(context, callback(*inparams))
                except KeyboardInterrupt:
                    sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT)
                    return
                except Exception, e:
                    msg = "user-defined function raised exception: "+str(e)
                    sqlite.sqlite3_result_error(context, msg, len(msg))

            c_closure = ffi.callback("void(sqlite3_context*,int,sqlite3_value**)", closure)
            self.func_cache[callback] = c_closure, closure
        try:
            ret = sqlite.sqlite3_create_function(self._db, name, num_args,
                                                 SQLITE_UTF8, ffi.NULL,
                                                 c_closure,
                                                 ffi.NULL,
                                                 ffi.NULL)
        except KeyboardInterrupt :
            msg = str('Keyboard Interrupt')
            raise self.OperationalError(msg)

        if ret != SQLITE_OK:
            raise self.OperationalError("Error creating function")


    def createaggregatefunction(self, name, cls, num_args = -1):
        try:
            step_callback, final_callback = self._aggregates[cls]
        except KeyError:
            @ffi.callback("void(sqlite3_context*, int, sqlite3_value**)")
            def step_callback(context, nargs, params):
                aggregate_ptr = ffi.cast("size_t[1]",
                    sqlite.sqlite3_aggregate_context(
                    context, ffi.sizeof("size_t")))

                if not aggregate_ptr[0]:
                    try:
                        aggregate, _, _ = cls()
                    except KeyboardInterrupt:
                        sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT);
                    except Exception:
                        msg = ("user-defined aggregate's '__init__' "
                               "method raised error")
                        sqlite.sqlite3_result_error(context, msg, len(msg))
                        return
                    aggregate_id = id(aggregate)
                    self.aggregate_instances[aggregate_id] = aggregate
                    aggregate_ptr[0] = aggregate_id

                if nargs == 1:
                    t = sqlite.sqlite3_value_type(params[0])
                    if t == 1:
                        inparams = sqlite.sqlite3_value_int64(params[0])
                    elif t == 2:
                        inparams = sqlite.sqlite3_value_double(params[0])
                    elif t == 3:
                        inparams = utf_8_decode(ffi.string(sqlite.sqlite3_value_text(params[0])))[0]
                    elif t == 4:
                        inparams = buffer(ffi.buffer(sqlite.sqlite3_value_blob(params[0]), sqlite.sqlite3_value_bytes(params[0]))[:])
                    else:
                        inparams = None

                    try:
                        self.aggregate_instances[aggregate_ptr[0]].step(inparams)
                    except KeyboardInterrupt:
                        sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT)
                    except Exception:
                        msg = ("user-defined aggregate's 'step' "
                               "method raised error")
                        sqlite.sqlite3_result_error(context, msg, len(msg))
                    return

                inparams = newlist_hint(nargs)
                ci = 0
                while ci < nargs:
                    # inparams.append(self._sqlite_to_python_value[sqlite.sqlite3_value_type(params[ci])](params[ci]))
                    # inparams.append(_sqlite_to_python(params[ci]))
                    t = sqlite.sqlite3_value_type(params[ci])
                    if t == 1:
                        inparams.append(sqlite.sqlite3_value_int64(params[ci]))
                    elif t == 2:
                        inparams.append(sqlite.sqlite3_value_double(params[ci]))
                    elif t == 3:
                        inparams.append(utf_8_decode(ffi.string(sqlite.sqlite3_value_text(params[ci])))[0])
                    elif t == 4:
                        inparams.append(buffer(ffi.buffer(sqlite.sqlite3_value_blob(params[ci]), sqlite.sqlite3_value_bytes(params[ci]))[:]))
                    else:
                        inparams.append(None)
                    ci += 1

                try:
                    self.aggregate_instances[aggregate_ptr[0]].step(*inparams)
                except KeyboardInterrupt:
                        sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT)
                except Exception:
                    msg = ("user-defined aggregate's 'step' "
                           "method raised error")
                    sqlite.sqlite3_result_error(context, msg, len(msg))

            @ffi.callback("void(sqlite3_context*)")
            def final_callback(context):
                aggregate_ptr = ffi.cast("size_t[1]",
                    sqlite.sqlite3_aggregate_context(
                    context, ffi.sizeof("size_t")))

                if aggregate_ptr[0]:
                    aggregate = self.aggregate_instances[aggregate_ptr[0]]

                    try:
                        val = aggregate.final()
                    except KeyboardInterrupt:
                        sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT);
                    except Exception:
                        msg = ("user-defined aggregate's 'finalize' "
                               "method raised error")

                        sqlite.sqlite3_result_error(context, msg, len(msg))
                    else:
                        try:
                            _python_to_sqlite(context, val)
                        except KeyboardInterrupt:
                            sqlite.sqlite3_result_error_code(context, SQLITE_INTERRUPT)

                    finally:
                        del self.aggregate_instances[aggregate_ptr[0]]

            self._aggregates[cls] = (step_callback, final_callback)

        ret = sqlite.sqlite3_create_function(self._db, name, num_args,
                                             SQLITE_UTF8, ffi.NULL,
                                             ffi.NULL,
                                             step_callback,
                                             final_callback)
        if ret != SQLITE_OK:
            raise self._get_exception(ret)

    def createmodule(self, name, datasource):
        self._vtdatasource[datasource] = datasource

            # Represents a table
        try:
            vtmodule = self._vtmodules[name](0)
        except KeyError:
            closure = lambda x:x
            closure.lastrow = None
            closure.lastcursorid = None

            def xCreate(db, paux, argc, argv, ppvtab, pzErr): #int (*xCreate)(sqlite3*, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char**);
                try:
                    schema, table = datasource.Create(self, ffi.string(argv[0]), ffi.string(argv[1]), ffi.string(argv[2]), *tuple([ffi.string(argv[i]) for i in xrange(3,argc)]))
                except Exception as e:
                    error_msg = str(e)
                    if "Error:" not in error_msg:
                        error_msg = type(e).__name__ +": " + error_msg
                    pzErr[0] = sqlite.sqlite3_mprintf(error_msg)
                    return SQLITE_ERROR

                newvtab = ffi.new("sqlite3_vtab *")
                ppvtab[0] = newvtab
                self._vttables[newvtab] = table
                vret = sqlite.sqlite3_declare_vtab(db, schema.encode('utf-8'))
                if ret != SQLITE_OK:
                    pzErr[0] = sqlite.sqlite3_mprintf(str(self._get_exception(vret)))
                    return SQLITE_ERROR
                return SQLITE_OK
            xConnect = xCreate

            def xBestIndex(pvtab, pInfo): #int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*);
                orderby = []
                constraints = []

                for i in xrange(pInfo.nOrderBy):
                    orderby.append((pInfo.aOrderBy[i].iColumn, pInfo.aOrderBy[i].desc))

                for ci in xrange(pInfo.nConstraint):
                    if pInfo.aConstraint[ci].usable:
                        constraints.append((pInfo.aConstraint[ci].iColumn, pInfo.aConstraint[ci].op))
                constraints, idxNum, idxStr, orderByConsumed, estimatedCost=self._vttables[pvtab].BestIndex(tuple(constraints), tuple(orderby))

                pInfo.idxNum = idxNum
                pInfo.orderByConsumed = orderByConsumed
                pInfo.estimatedCost = estimatedCost

                if idxStr:
                    pInfo.idxStr = sqlite.sqlite3_mprintf(idxStr.encode('utf-8'))
                    pInfo.needToFreeIdxStr = 1

                if constraints:
                    pci = 0
                    for ci in xrange(pInfo.nConstraint):
                        if pInfo.aConstraint[ci].usable:
                            c = constraints[pci]
                            if c:
                                if type(c) in (list, tuple):
                                    pInfo.aConstraintUsage[ci].argvIndex = c[0] + 1
                                    pInfo.aConstraintUsage[ci].omit = c[1]
                                else:
                                    pInfo.aConstraintUsage[ci].argvIndex = c + 1
                            pci += 1

                return SQLITE_OK

            def xDisconnect(pvtab): #int (*xDisconnect)(sqlite3_vtab *pVTab);
                if hasattr(self._vttables[pvtab], 'xDisconnect'):
                    self._vttables[pvtab].xDisconnect()
                __delVT__(pvtab)
                return SQLITE_OK


            def xDestroy(pvtab): #int (*xDisconnect)(sqlite3_vtab *pVTab);
                self._vttables[pvtab].Destroy()
                __delVT__(pvtab)
                return SQLITE_OK

            def __delVT__(pvtab):
                del self._vttables[pvtab]
                try:
                    for i in self._vtcursortables:
                        if self._vtcursortables == pvtab:
                            xCloseID(i)
                except KeyboardInterrupt:
                    return SQLITE_INTERRUPT
                except:
                    pass
                return SQLITE_OK


            def xOpen(pvtab, ppcursor):  # int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
                newcursor = ffi.new("sqlite3_vtab_cursor *")
                instance = self._vttables[pvtab].Open()
                length = len(self._vtcursors)

                try:
                    newcursor.n = self._vtcursors.index(None)
                except:
                    newcursor.n = length
                i = newcursor.n

                if i == length:
                    self._vtcursors.append(newcursor)
                    self._vtcursorinstances += tuple([instance])
                    self._vtcursorcolumn.append(instance.Column)
                    self._vtcursoreof += tuple([instance.Eof])
                    self._vtcursortables.append(pvtab)
                else:
                    self._vtcursors[i] = newcursor

                    tmp = list(self._vtcursorinstances)
                    tmp[i] = instance
                    self._vtcursorinstances = tuple(tmp)

                    tmp = list(self._vtcursoreof)
                    tmp[i] = instance.Eof
                    self._vtcursoreof = tuple(tmp)

                    self._vtcursorcolumn[i] = instance.Column
                    self._vtcursortables[i] = pvtab

                ppcursor[0] = newcursor
                return SQLITE_OK

            def xClose(vtabcursor): # int (*xClose)(sqlite3_vtab_cursor*);
                try:
                    self._vtcursors[vtabcursor.n] = None
                    self._vtcursorcolumn[vtabcursor.n] = None
                    tmp = list(self._vtcursoreof)
                    tmp[vtabcursor.n] = None
                    self._vtcursoreof = tuple(tmp)
                    self._vtcursorinstances[vtabcursor.n].Close()
                    tmp = list(self._vtcursorinstances)
                    tmp[vtabcursor.n] = None
                    self._vtcursorinstances = tuple(tmp)
                    self._vtcursortables[vtabcursor.n] = None
                except:
                    pass

                return SQLITE_OK

            def xCloseID(n):
                # try:
                self._vtcursors[n] = None
                self._vtcursorcolumn[n] = None
                tmp = list(self._vtcursoreof)
                tmp[n] = None
                self._vtcursoreof = tuple(tmp)
                self._vtcursorinstances[n].Close()
                tmp = list(self._vtcursorinstances)
                tmp[n] = None
                self._vtcursorinstances = tuple(tmp)
                self._vtcursortables[n] = None
                # except:
                #     pass

                return SQLITE_OK

            def xFilter(vtabcursor, idxnum, cidxstr, argc, argv) : # int (*xFilter)(sqlite3_vta788b_cursor*, int idxNum, const char *idxStr, int argc, sqlite3_value **argv);
                try:
                    if cidxstr == ffi.NULL:
                        idxstr = None
                    else:
                        idxstr = ffi.string(cidxstr)
                    constraints = newlist_hint(argc)
                    ci = 0
                    while ci < argc:
                        t = sqlite.sqlite3_value_type(argv[ci])
                        if t == 1:
                            constraints.append(sqlite.sqlite3_value_int64(argv[ci]))
                        elif t == 2:
                            constraints.append(sqlite.sqlite3_value_double(argv[ci]))
                        elif t == 3:
                            constraints.append(utf_8_decode(ffi.string(sqlite.sqlite3_value_text(argv[ci])))[0])
                        elif t == 4:
                            constraints.append(buffer(ffi.buffer(sqlite.sqlite3_value_blob(argv[ci]), sqlite.sqlite3_value_bytes(argv[ci]))[:]))
                        else:
                            constraints.append(None)
                        ci += 1
                    self._vtcursorinstances[vtabcursor.n].Filter(idxnum, idxstr, tuple(constraints))
                except KeyboardInterrupt, e:
                    self._vttables[vtabcursor.pVtab].Disconnect()
                    return SQLITE_INTERRUPT
                except Exception, e:
                    vtabcursor.pVtab.zErrMsg = sqlite.sqlite3_mprintf(unicode(e).encode('utf-8'))
                    return SQLITE_ERROR

                return 0 #return SQLITE_OK

            def xNext(vtabcursor): #int (*xNext)(sqlite3_vtab_cursor*)
                #print 'xNext'
                #print vtabcursor.n
                try:
                    self._vtcursorinstances[vtabcursor.n].Next()
                    return SQLITE_OK
                except KeyboardInterrupt:
                    return SQLITE_INTERRUPT

            def xEof(vtabcursor): #int (*xEof)(sqlite3_vtab_cursor*);
                try:
                    # return self._vtcursoreof[vtabcursor.n]()
                    return self._vtcursorinstances[vtabcursor.n].eof
                except KeyboardInterrupt:
                    return SQLITE_INTERRUPT
                except:
                    return 1

            def xNextFast(vtabcursor):
                try:
                    self._vtcursorinstances[vtabcursor.n].Next()
                except KeyboardInterrupt:
                    return SQLITE_INTERRUPT
                return 0 #return SQLITE_OK

            def xColumnFast(vtabcursor, context, col):
                _python_to_sqlitedict[type(self._vtcursorinstances[vtabcursor.n].row[col])](context, self._vtcursorinstances[vtabcursor.n].row[col])
                return 0
                # val = self._vtcursorinstances[vtabcursor.n].row[col]

                # cl = type(_val)
                # if cl is unicode:
                #     sqlite.sqlite3_result_text(context, _val.encode('utf-8'), -1, SQLITE_TRANSIENT)
                # else:
                #     _python_to_sqlitedict[cl](context, _val)
                # return 0

                # cl = type(val)
                # if cl is unicode:
                #     sqlite.sqlite3_result_text(context, val.encode('utf-8'), -1, SQLITE_TRANSIENT)
                # elif cl is int:
                #     sqlite.sqlite3_result_int64(context, val)
                # elif cl is float:
                #     sqlite.sqlite3_result_double(context, val)
                # elif cl is str:
                #     sqlite.sqlite3_result_text(context, val, -1, SQLITE_TRANSIENT)
                # elif cl is bool:
                #     sqlite.sqlite3_result_int(context, val)
                # elif cl is long:
                #     sqlite.sqlite3_result_text(context, str(val), -1, SQLITE_TRANSIENT)
                # elif cl is NoneType:
                #     sqlite.sqlite3_result_null(context)
                # elif cl is buffer:
                #     sqlite.sqlite3_result_blob(context, str(val), len(val), SQLITE_TRANSIENT)
                # return 0 #return SQLITE_OK

            def xColumn(vtabcursor, context, col):  # int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context*, int);
                _python_to_sqlite(context, self._vtcursorcolumn[vtabcursor.n](col))
                return SQLITE_OK

            def xRowid(vtabcursor, pRowid): # int (*xRowid)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid);
                pRowid[0] = ffi.cast('sqlite3_int64', self._vtcursorinstances[vtabcursor.n].Rowid())

            def xUpdate(pvtab, val, num) : #int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *);
                return SQLITE_OK

            def xCommit(pvtab): #int (*xCommit)(sqlite3_vtab *pVTab);
                return SQLITE_OK

            def xSync(pvtab): #int (*xCommit)(sqlite3_vtab *pVTab);
                return SQLITE_OK

            def xBegin(pvtab): #int (*xCommit)(sqlite3_vtab *pVTab);
                return SQLITE_OK

            def xRollback(pvtab): #  int (*xRollback)(sqlite3_vtab *pVTab);
                print 17

            def xRename(pvtab, znew): #  int (*xRename)(sqlite3_vtab *pVtab, const char *zNew);
                print 18

            def xSavepoint(pvtab, num): #  int (*xSavepoint)(sqlite3_vtab *pVTab, int);
                print 19

            def xRelease(pvtab , num): #  int (*xRelease)(sqlite3_vtab *pVTab, int);
                print 20
                return SQLITE_OK

            def xRollbackTo(pvtab , num): #  int (*xRollbackTo)(sqlite3_vtab *pVTab, int);
                print 21

            vtmodule = ffi.new("sqlite3_module *")

            xCreateCallback = ffi.callback("int(sqlite3*, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char**)", xCreate)
            xConnectCallback = ffi.callback("int(sqlite3*, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char**)", xConnect)
            xBestIndexCallback  = ffi.callback("int(sqlite3_vtab *pVTab, sqlite3_index_info*)", xBestIndex)
            xDestroyCallback = ffi.callback("int(sqlite3_vtab *pVTab)", xDestroy)
            xDisconnectCallback = ffi.callback("int(sqlite3_vtab *pVTab)", xDisconnect)
            xOpenCallback = ffi.callback("int(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursfactoryor)", xOpen)
            xCloseCallback = ffi.callback("int(sqlite3_vtab_cursor*)", xClose)
            xFilterCallback = ffi.callback("int(sqlite3_vtab_cursor*, int idxNum, const char *idxStr, int argc, sqlite3_value **argv)", xFilter, SQLITE_ERROR)
            xEofCallback = ffi.callback("int(sqlite3_vtab_cursor*)", xEof)
            xRowidCallback = ffi.callback("int(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid)", xRowid)
            xUpdateCallback = ffi.callback("int(sqlite3_vtab *, int, sqlite3_value **, sqlite3_int64 *)", xUpdate)
            xBeginCallback = ffi.NULL #ffi.callback("int(sqlite3_vtab *pVTab)", xBegin)
            xSyncCallback = ffi.NULL # ffi.callback("int(sqlite3_vtab *pVTab)", xSync)
            xCommitCallback = ffi.NULL #ffi.callback("int(sqlite3_vtab *pVTab)", xCommit)
            xRollbackCallback = ffi.callback("int(sqlite3_vtab *pVTab)", xRollback)
            xRenameCallback = ffi.callback("int(sqlite3_vtab *pVtab, const char *zNew)", xRename)
            xSavepointCallback = ffi.callback("int(sqlite3_vtab *pVTab, int)", xSavepoint)
            xReleaseCallback = ffi.callback("int(sqlite3_vtab *pVTab, int)", xRelease)
            xRollbackToCallback = ffi.callback("int(sqlite3_vtab *pVTab, int)", xRollbackTo)
            xFindFunctionCallback = ffi.NULL

            if '_madisVT' in datasource.__dict__ and datasource._madisVT == True :
                xNextCallback = ffi.callback("int(sqlite3_vtab_cursor*)", xNextFast)
                xColumnCallback = ffi.callback("int(sqlite3_vtab_cursor*, sqlite3_context*, int)", xColumnFast)
            else:
                xNextCallback = ffi.callback("int(sqlite3_vtab_cursor*)", xNext)
                xColumnCallback = ffi.callback("int(sqlite3_vtab_cursor*, sqlite3_context*, int)", xColumn)

            vtmodule.xCreate = xCreateCallback
            vtmodule.xConnect =  xConnectCallback
            vtmodule.xBestIndex = xBestIndexCallback
            vtmodule.xDisconnect = xDisconnectCallback
            vtmodule.xDestroy = xDestroyCallback
            vtmodule.xOpen = xOpenCallback
            vtmodule.xClose = xCloseCallback
            vtmodule.xFilter = xFilterCallback
            vtmodule.xNext = xNextCallback
            vtmodule.xEof = xEofCallback
            vtmodule.xColumn = xColumnCallback
            vtmodule.xRowid = xRowidCallback
            vtmodule.xUpdate = xUpdateCallback
            vtmodule.xBegin = xBeginCallback
            vtmodule.xSync = xSyncCallback
            vtmodule.xCommit = xCommitCallback
            vtmodule.xRollback = xRollbackCallback
            vtmodule.xRename = xRenameCallback
            vtmodule.xSavepoint = xSavepointCallback
            vtmodule.xRelease = xReleaseCallback
            vtmodule.xRollbackTo = xRollbackToCallback
            vtmodule.xFindFunction = xFindFunctionCallback

            self._vtmodules[datasource] = \
        (vtmodule, xCreate, xConnect, xBestIndex, xDisconnect, xDestroy, xOpen,
            xClose, xFilter, xNext, xEof, xColumn, xRowid, xUpdate, xBegin, xSync, xCommit, xRollback, xRename,
            xSavepoint, xRelease, xRollbackTo, xCreateCallback, xConnectCallback, xBestIndexCallback,
            xDestroyCallback, xDisconnectCallback, xOpenCallback, xCloseCallback, xFilterCallback, xNextCallback,
            xEofCallback, xColumnCallback, xRowidCallback, xUpdateCallback, xBeginCallback, xSyncCallback,
            xCommitCallback, xRollbackCallback, xRenameCallback, xSavepointCallback, xReleaseCallback,
            xRollbackToCallback, xFindFunctionCallback, xNextFast, xColumnFast
        )

#        int sqlite3_create_module_v2(
#  sqlite3 *db,               /* SQLite connection to register module with */
#  const char *zName,         /* Name of the module */
#  const sqlite3_module *p,   /* Methods for the module */
#  void *pClientData,         /* Client data for xCreate/xConnect */
#  void(*xDestroy)(void*)     /* Module destructor function */
#  );

        ret = sqlite.sqlite3_create_module(self._db, name , vtmodule ,ffi.NULL)
        if ret != SQLITE_OK:
            raise self._get_exception(ret)


    def iterdump(self):
        from sqlite3.dump import _iterdump
        return _iterdump(self)

    #if HAS_LOAD_EXTENSION:
    def enableloadextension(self, enabled):
            return
            rc = sqlite.sqlite3_enable_load_extension(self._db, int(enabled))

            if rc != SQLITE_OK:
                raise OperationalError("Error enabling load extension")

DML, DQL, DDL = range(3)


class CursorLock(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        if self.cursor.locked:
            raise ProgrammingError("Recursive use of cursors not allowed.")
        self.cursor.locked = True

    def __exit__(self, *args):
        self.cursor.locked = False


class Cursor(object):
    def __init__(self, con):
        if not isinstance(con, Connection):
            raise TypeError
        con._check_closed()
        con.cursors.append(weakref.ref(self))
        self.connection = con
        self._description = None
        self.arraysize = 1
        self.rowcount = -1
        self.statement = None
        self.reset = False
        self.locked = False
        self.next_char = None
        self.params = None
        self.column_count = 0
        self.connection = con
        self._description = None
        self.exhausted = False
        self.iter = None
        self.next = self.__iter__().next

    def setexectrace(self, f):
        self.connection.exectracefun = f

    def _check_closed(self):
        if not getattr(self, 'connection', None):
            raise ProgrammingError("Cannot operate on a closed cursor.")
        self.connection._check_closed()

    def _check_and_lock(self):
        return CursorLock(self)

    def execute(self, sql, params=None):
        self.params = params
        if self.locked:
            raise ProgrammingError("Recursive use of cursors not allowed.")
        self.locked = True
        self._description = None
        self.reset = False

        try:
            self.statement, self.next_char = self.connection.statement_cache.get(sql)
        except:
            raise

        try:
            self.exhausted = True
            while True:
                if self.connection.exectracefun:
                    if self.connection.exectracefun(self, self.statement, params) == 0:
                        return
                self.statement.set_params(params)
                stst = self.statement.statement

                # Actually execute the SQL statement
                ret = sqlite.sqlite3_step(stst)

                if ret != SQLITE_DONE:
                    break

                if self.next_char == '':
                    self.locked = False
                    return self

                self.statement, self.next_char = self.connection.statement_cache.get(self.next_char)
                if self.statement is None:
                    self.locked = False

            if ret == SQLITE_ROW:
                self.exhausted = False
                self.locked = False
                self.column_count = sqlite.sqlite3_column_count(stst)
                return self

            self.statement.reset()
            self.reset = True
            raise self.connection._get_exception(ret)
        except:
            self.locked = False
            raise

        if self.exhausted:
            self.locked = False

        return self

    def __iter__(self):
        while not self.exhausted:
            ret = SQLITE_ROW
            stst = self.statement.statement
            self.column_count = sqlite.sqlite3_column_count(stst)
            scount = self.column_count + 0
            row = [None] * scount
            l_sqlite_to_python_statement = tuple([
                None,
                sqlite.sqlite3_column_int64,
                sqlite.sqlite3_column_double,
                lambda st, c: utf_8_decode(ffi.string(sqlite.sqlite3_column_text(st, c)))[0],
                lambda st, c: buffer(ffi.buffer(sqlite.sqlite3_column_blob(st, c), sqlite.sqlite3_column_bytes(st, c))[:]),
                lambda st, c: None
            ])

            while ret == 100:  #SQLITE_ROW:
                ci = 0
                while ci < scount:
                    row[ci] = l_sqlite_to_python_statement[sqlite.sqlite3_column_type(stst, ci)](stst, ci)
                    ci += 1

                ret = sqlite.sqlite3_step(stst)
                yield tuple(row)

            if ret == SQLITE_DONE:
                if self.next_char != '':
                    self.statement, self.next_char = self.connection.statement_cache.get(self.next_char)
                    if self.statement is None:
                        self.locked = False
                        raise StopIteration
                    while True:
                        self.statement.set_params(self.params)

                        # Actually execute the SQL statement
                        ret = sqlite.sqlite3_step(self.statement.statement)

                        if ret != SQLITE_DONE:
                            break

                        if self.next_char == '':
                            self.locked = False
                            raise StopIteration

                        self.statement, self.next_char = self.connection.statement_cache.get(self.next_char)
                        if self.statement is None:
                            self.locked = False
                else:
                    self.exhausted = True
            else:
                exc = self.connection._get_exception(ret)
                self.statement.reset()
                raise exc

        self.locked = False
        raise StopIteration

    def prepare(self, sql):
            if type(sql) is unicode:
                sql = sql.encode("utf-8")
            self._description = None
            self.reset = False

            try:
                stat = Statement(self.connection, sql)
            except:
                raise

            return stat

    def executedirect(self, st, params=None):
        i1 = 1
        for p in params:
            cl = type(p)
            if cl is unicode:
                sqlite.sqlite3_bind_text(st.statement, i1, p.encode('utf-8'), -1, SQLITE_TRANSIENT)
            elif cl is int:
                sqlite.sqlite3_bind_int64(st.statement, i1, p)
            elif cl is float:
                sqlite.sqlite3_bind_double(st.statement, i1, p)
            elif cl is str:
                sqlite.sqlite3_bind_text(st.statement, i1, p, -1, SQLITE_TRANSIENT)
            elif cl is NoneType:
                sqlite.sqlite3_bind_null(st.statement, i1)
            elif cl is bool:
                sqlite.sqlite3_bind_int(st.statement, i1, p)
            elif cl is long:
                sqlite.sqlite3_bind_text(st.statement, i1, str(p), -1, SQLITE_TRANSIENT)
            elif cl is buffer:
                sqlite.sqlite3_bind_blob(st.statement, i1, str(p), len(p), SQLITE_TRANSIENT)
            i1 += 1

        # Actually execute the SQL statement
        sqlite.sqlite3_step(st.statement)
        sqlite.sqlite3_reset(st.statement)

    def executemany(self, sql, many_params):
        if type(sql) is unicode:
            sql = sql.encode("utf-8")

        with self._check_and_lock():
            self._description = None
            self.reset = False
            self.statement, next = self.connection.statement_cache.get(sql)

            if self.statement.kind == DQL:
                self.connection._begin()
            else:
                raise ProgrammingError("executemany is only for DML statements")

            st = self.statement.statement
            sqlite_transient = ffi.cast("const void *", -1)
            for params in many_params:
                sqlite.sqlite3_reset(st)
                i1 = 1
                for p in params:
                    cl = type(p)
                    if cl is unicode:
                        sqlite.sqlite3_bind_text(st, i1, p.encode('utf-8'), -1, sqlite_transient)
                    elif cl is int:
                        sqlite.sqlite3_bind_int64(st, i1, p)
                    elif cl is float:
                        sqlite.sqlite3_bind_double(st, i1, p)
                    elif cl is NoneType:
                        sqlite.sqlite3_bind_null(st, i1)
                    elif cl is bool:
                        sqlite.sqlite3_bind_int(st, i1, p)
                    elif cl is long:
                        sqlite.sqlite3_bind_text(st, i1, str(p), -1, sqlite_transient)
                    elif cl is str:
                        sqlite.sqlite3_bind_text(st, i1, p, -1, sqlite_transient)
                    elif cl is buffer:
                        sqlite.sqlite3_bind_blob(st, i1, str(p), len(p), sqlite_transient)
                    i1 += 1

                # Actually execute the SQL statement
                ret = sqlite.sqlite3_step(st)
                if ret != SQLITE_DONE:
                    raise self.connection._get_exception(ret)

        return self

    def executesplit(self, sql):
        if type(sql) is unicode:
            sql = sql.encode("utf-8")

        with self._check_and_lock():
            self._description = None
            self.reset = False
            self.statement, next = self.connection.statement_cache.get(sql)

            if self.statement.kind == DQL:
                self.connection._begin()
            else:
                raise ProgrammingError("executemany is only for DML statements")

            st = self.statement.statement
            sqlite_transient = ffi.cast("const void *", -1)
            params = yield()
            plen = len(params)
            while True:
                sqlite.sqlite3_reset(st)
                i1 = 1
                while i1 < plen:
                    p = params[i1]
                    cl = type(p)
                    if cl is unicode:
                        sqlite.sqlite3_bind_text(st, i1, p.encode('utf-8'), -1, sqlite_transient)
                    elif cl is int:
                        sqlite.sqlite3_bind_int64(st, i1, p)
                    elif cl is float:
                        sqlite.sqlite3_bind_double(st, i1, p)
                    elif cl is NoneType:
                        sqlite.sqlite3_bind_null(st, i1)
                    elif cl is bool:
                        sqlite.sqlite3_bind_int(st, i1, p)
                    elif cl is long:
                        sqlite.sqlite3_bind_text(st, i1, str(p), -1, sqlite_transient)
                    elif cl is str:
                        sqlite.sqlite3_bind_text(st, i1, p, -1, sqlite_transient)
                    elif cl is buffer:
                        sqlite.sqlite3_bind_blob(st, i1, str(p), len(p), sqlite_transient)
                    i1 += 1
                ret = sqlite.sqlite3_step(st)
                params = yield()
                # Actually execute the SQL statement
                if ret != SQLITE_DONE:
                    raise self.connection._get_exception(ret)


    def executescript(self, sql):
        self._description = None
        self.reset = False
        if type(sql) is unicode:
            sql = sql.encode("utf-8")

        statement = None
        statement_p = ffi.new("sqlite3_stmt **", statement)

        c_sql = ffi.new("char[]", sql)

        c_sql_p = ffi.new("char**", c_sql)

        self.connection.commit()
        while True:
            rc = sqlite.sqlite3_prepare(self.connection._db, c_sql, -1, statement_p, c_sql_p)

            c_sql = c_sql_p[0]
            statement = statement_p[0]
            if rc != SQLITE_OK:
                raise self.connection._get_exception(rc)

            rc = SQLITE_ROW
            while rc == SQLITE_ROW:
                if not statement:
                    rc = SQLITE_OK
                else:
                    rc = sqlite.sqlite3_step(statement)

            if rc != SQLITE_DONE:
                sqlite.sqlite3_finalize(statement)
                if rc == SQLITE_OK:
                    return self
                else :
                    raise self.connection._get_exception(rc)
            rc = sqlite.sqlite3_finalize(statement)
            if rc != SQLITE_OK:
                raise self.connection._get_exception(rc)

            if not c_sql:
                break
        return self

    def _check_reset(self):
        if self.reset:
            raise self.connection.InterfaceError("Cursor needed to be reset because "
                                                 "of commit/rollback and can "
                                                 "no longer be fetched from.")

    # do all statements
    def fetchone(self):
        if self.statement is None:
            return None

        try:
            return self.next()
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if self.statement is None:
            return []
        if size is None:
            size = self.arraysize
        lst = []
        for row in self:
            lst.append(row)
            if len(lst) == size:
                break
        return lst

    def fetchall(self):
        if self.statement is None:
            return []
        return list(self)

    def _getdescription(self):
        if self._description is None:
            self._description = self.statement._get_description()
        return self._description

    def _getlastrowid(self):
        return sqlite.sqlite3_last_insert_rowid(self.connection._db)

    def close(self, *args):
        if not self.connection:
            return

        if self.statement:
            self.statement.reset()
            self.statement = None
        self.connection.cursors.remove(weakref.ref(self))
        self.connection = None

    def setinputsizes(self, *args):
        pass
    def setoutputsize(self, *args):
        pass

    description = property(_getdescription)
    getdescription = _getdescription
    lastrowid = property(_getlastrowid)

class Statement(object):
    def __init__(self, connection, sql):
        self.statement = None
        self.column_count = None

        if type(sql) is unicode:
            sql = sql.encode("utf-8")
        if not isinstance(sql, str):
            raise ValueError("sql must be a string")

        self.con = connection
        self.sql = sql # DEBUG ONLY
        self.kind = DQL
        self.exhausted = False
        self.in_use = False
        self.statement_p = ffi.new("sqlite3_stmt **", self.statement)
        self.next_char = ffi.new("char[]", "")
        mynext_char = ffi.new("char**", self.next_char)
        sql_char = ffi.new("char[]", sql)

        try:
            ret = sqlite.sqlite3_prepare_v2(self.con._db, sql_char, -1, self.statement_p, mynext_char)

        except Exception, e:
            raise SQLError(e)

        self.next_char = ffi.string(mynext_char[0])
        self.statement = self.statement_p[0]

        if ret == SQLITE_OK and self.statement is None:

            # an empty statement, we work around that, as it's the least trouble
            try:
                ret = sqlite.sqlite3_prepare_v2(self.con._db, "select 42", -1, self.statement_p, self.next_char)
            except:

                raise

            self.kind = DQL

        if ret != SQLITE_OK:
            raise self.con._get_exception(ret)

        self.con._remember_statement(self)

    def set_params(self, params):
        ret = sqlite.sqlite3_reset(self.statement)

        if ret != SQLITE_OK:
            raise self.con._get_exception(ret)
        self.mark_dirty()

        if params is None:
            if sqlite.sqlite3_bind_parameter_count(self.statement) != 0:
                raise ProgrammingError("wrong number of arguments")
            return

        if type(params) is not dict:
            if len(params) != sqlite.sqlite3_bind_parameter_count(self.statement):
                raise ProgrammingError("wrong number of arguments")

            for ci in xrange(len(params)):
                _bind_param[type(params[ci])](self.statement, ci+1, params[ci])
        else:
            for idx in xrange(1, sqlite.sqlite3_bind_parameter_count(self.statement) + 1):
                param_name = sqlite.sqlite3_bind_parameter_name(self.statement, idx)
                if param_name is None:
                    raise ProgrammingError("need named parameters")
                param_name = param_name[1:]
                try:
                    param = params[param_name]
                except KeyError:
                    raise ProgrammingError("missing parameter '%s'" % param)
                _bind_param[type(param)](self.statement, idx, param)

    def reset(self):
        self.row_cast_map = None
        ret = sqlite.sqlite3_reset(self.statement)
        self.in_use = False
        self.exhausted = False
        return ret

    def finalize(self):
        sqlite.sqlite3_finalize(self.statement)
        self.statement = None
        self.in_use = False

    def mark_dirty(self):
        self.in_use = True

    def __del__(self):
        if self.statement != None :
            sqlite.sqlite3_finalize(self.statement)
        self.statement = None

    def _get_description(self):
        if self.kind == DML:
            return ()
        desc = []
        for ci in xrange(sqlite.sqlite3_column_count(self.statement)):
            name = ffi.string(sqlite.sqlite3_column_name(self.statement, ci)).split("[")[0].strip()
            desc.append((name, None))
        return desc

def _sqlite_to_python(val):
    t = sqlite.sqlite3_value_type(val)
    if t < 2:
        if t == 1:
            return sqlite.sqlite3_value_int64(val)
        else:
            return sqlite.sqlite3_value_double(val)
    else:
        if t == 3:
            return utf_8_decode(ffi.string(sqlite.sqlite3_value_text(val)))[0]
        else:
            if t == 4:
                return buffer(ffi.buffer(sqlite.sqlite3_value_blob(val), sqlite.sqlite3_value_bytes(val))[:])
            else:
                return None

def _python_to_sqlite(context, val):
    cl = type(val)
    if cl is unicode:
        sqlite.sqlite3_result_text(context, val.encode('utf-8'), -1, SQLITE_TRANSIENT)
    elif cl is int:
        sqlite.sqlite3_result_int64(context, val)
    elif cl is str:
        sqlite.sqlite3_result_text(context, val, -1, SQLITE_TRANSIENT)
    elif cl is float:
        sqlite.sqlite3_result_double(context, val)
    elif cl is bool:
        sqlite.sqlite3_result_int(context, val)
    elif cl is long:
        sqlite.sqlite3_result_text(context, str(val), len(val), SQLITE_TRANSIENT)
    elif val is None:
        sqlite.sqlite3_result_null(context)
    elif cl is buffer:
        sqlite.sqlite3_result_blob(context, str(val), len(val), SQLITE_TRANSIENT)

_sqlite_to_python_value = tuple([
    None,
    sqlite.sqlite3_value_int64,
    sqlite.sqlite3_value_double,
    lambda x: ffi.string(sqlite.sqlite3_value_text(x)).decode('utf-8'),
    lambda x: buffer(ffi.buffer(sqlite.sqlite3_value_blob(x), sqlite.sqlite3_value_bytes(x))[:]),
    lambda x: None
])

_bind_param = {
    unicode: lambda st, idx, param: sqlite.sqlite3_bind_text(st, idx, param.encode('utf-8'), -1, SQLITE_TRANSIENT),
    int: sqlite.sqlite3_bind_int64,
    float: sqlite.sqlite3_bind_double,
    str: lambda st, idx, param: sqlite.sqlite3_bind_text(st, idx, param, len(param), SQLITE_TRANSIENT),
    long: lambda st, idx, param: sqlite.sqlite3_bind_text(st, idx, str(param), len(str(param)), SQLITE_TRANSIENT),
    type(None): lambda st, idx, param: sqlite.sqlite3_bind_null(st, idx),
    bool: sqlite.sqlite3_bind_int,
    buffer: lambda st, idx, param: sqlite.sqlite3_bind_blob(st, idx, str(param), len(param), SQLITE_TRANSIENT)
}

_sqlite_to_python_statement = tuple([
    None,
    lambda st, ci: sqlite.sqlite3_column_int64(st, ci),
    lambda st, ci: sqlite.sqlite3_column_double(st, ci),
    lambda st, ci: unicode(ffi.string(sqlite.sqlite3_column_text(st, ci))),
    lambda st, ci: buffer(ffi.buffer(sqlite.sqlite3_column_blob(
        st, ci), sqlite.sqlite3_column_bytes(st, ci))[:]),
    lambda st, ci: None
])

_python_to_sqlitedict = {
    unicode: lambda c, val: sqlite.sqlite3_result_text(c, val.encode('utf-8'), -1, SQLITE_TRANSIENT),
    int: sqlite.sqlite3_result_int64,
    float: sqlite.sqlite3_result_double,
    str: lambda c, val: sqlite.sqlite3_result_text(c, val, -1, SQLITE_TRANSIENT),
    long: lambda c, val: sqlite.sqlite3_result_text(c, str(val), -1, SQLITE_TRANSIENT),
    NoneType: lambda c, val: sqlite.sqlite3_result_null(c),
    bool: sqlite.sqlite3_result_int,
    buffer: lambda c, val: sqlite.sqlite3_result_blob(c, str(val), len(val), SQLITE_TRANSIENT)
}