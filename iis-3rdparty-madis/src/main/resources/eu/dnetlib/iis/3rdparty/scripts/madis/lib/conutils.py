def optimized_cursor(connection,type='sqlite'):
    if type=='sqlite':
        cursor=connection.cursor()
        cursor.execute('PRAGMA count_changes=OFF')
        cursor.execute('PRAGMA page_cache=100000')
        cursor.execute('PRAGMA page_size=32768')
        cursor.execute('PRAGMA synchronous=1')
        cursor.execute('PRAGMA journal_mode=PERSIST')
        return cursor
