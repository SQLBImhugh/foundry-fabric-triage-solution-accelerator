"""Pure SQL JSON string quoting for the existing Python identity digests.

Python's identity digest uses ensure_ascii=True without escaped slashes. SQL's
FOR JSON/STRING_ESCAPE serialization is different. Iterate UTF-16 code units
under a non-SC binary collation so supplementary characters retain both escaped
surrogates, exactly as json.dumps does. This function has no table access.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_contracts import KernelObject, SqlNames


def json_string_function(names: SqlNames) -> KernelObject:
    logical = "json_identity_string"
    ddl = rf"""CREATE OR ALTER FUNCTION {names.object(logical)} (@text nvarchar(max))
RETURNS nvarchar(max)
WITH SCHEMABINDING
AS
BEGIN
    IF @text IS NULL RETURN NULL;
    DECLARE @result nvarchar(max)=N'"',@position int=1,@code int,@character nvarchar(1);
    WHILE @position<=DATALENGTH(@text)/2
    BEGIN
        SET @character=SUBSTRING(@text COLLATE Latin1_General_100_BIN2,@position,1);
        SET @code=UNICODE(@character);
        SET @result=@result+CASE @code
            WHEN 34 THEN N'\"' WHEN 92 THEN N'\\' WHEN 8 THEN N'\b'
            WHEN 9 THEN N'\t' WHEN 10 THEN N'\n' WHEN 12 THEN N'\f' WHEN 13 THEN N'\r'
            ELSE CASE WHEN @code BETWEEN 32 AND 126 THEN @character
                 ELSE N'\u'+LOWER(CONVERT(nvarchar(4),CONVERT(binary(2),@code),2)) END
        END;
        SET @position=@position+1;
    END;
    RETURN @result+N'"';
END;"""
    return KernelObject(logical, names.object(logical), "function", ddl)


def json_equal_function(names: SqlNames) -> KernelObject:
    """Bounded structural comparison; object ordering is not authorization."""
    logical = "json_equal"
    ddl = f"""CREATE OR ALTER FUNCTION {names.object(logical)} (@left nvarchar(max),@right nvarchar(max))
RETURNS bit
WITH SCHEMABINDING
AS
BEGIN
    IF @left IS NULL OR @right IS NULL OR ISJSON(N'['+@left+N']')<>1 OR ISJSON(N'['+@right+N']')<>1 RETURN 0;
    DECLARE @todo TABLE (id int IDENTITY(1,1),a nvarchar(max),b nvarchar(max),ta int,tb int);
    DECLARE @a TABLE (k nvarchar(4000),v nvarchar(max),t int);
    DECLARE @b TABLE (k nvarchar(4000),v nvarchar(max),t int);
    IF (SELECT COUNT(*) FROM OPENJSON(N'['+@left+N']'))<>1
       OR (SELECT COUNT(*) FROM OPENJSON(N'['+@right+N']'))<>1 RETURN 0;
    INSERT INTO @todo SELECT x.value,y.value,x.type,y.type
        FROM OPENJSON(N'['+@left+N']') AS x CROSS JOIN OPENJSON(N'['+@right+N']') AS y;
    DECLARE @index int=1,@av nvarchar(max),@bv nvarchar(max),@at int,@bt int;
    WHILE @index<=(SELECT COUNT(*) FROM @todo)
    BEGIN
        IF @index>8192 RETURN 0;
        SELECT @av=a,@bv=b,@at=ta,@bt=tb FROM @todo WHERE id=@index;
        IF @at<>@bt RETURN 0;
        IF @at BETWEEN 1 AND 3 AND (DATALENGTH(@av)<>DATALENGTH(@bv)
            OR @av COLLATE Latin1_General_100_BIN2<>@bv COLLATE Latin1_General_100_BIN2) RETURN 0;
        IF @at IN (4,5)
        BEGIN
            DELETE FROM @a; DELETE FROM @b;
            INSERT INTO @a SELECT [key],value,type FROM OPENJSON(@av);
            INSERT INTO @b SELECT [key],value,type FROM OPENJSON(@bv);
            IF (SELECT COUNT(*) FROM @a)<>(SELECT COUNT(*) FROM @b) RETURN 0;
            IF EXISTS (SELECT k COLLATE Latin1_General_100_BIN2,DATALENGTH(k) FROM @a
                GROUP BY k COLLATE Latin1_General_100_BIN2,DATALENGTH(k) HAVING COUNT(*)>1)
                OR EXISTS (SELECT k COLLATE Latin1_General_100_BIN2,DATALENGTH(k) FROM @b
                GROUP BY k COLLATE Latin1_General_100_BIN2,DATALENGTH(k) HAVING COUNT(*)>1) RETURN 0;
            IF EXISTS (SELECT 1 FROM @a AS x WHERE NOT EXISTS (SELECT 1 FROM @b AS y
                WHERE x.k COLLATE Latin1_General_100_BIN2=y.k COLLATE Latin1_General_100_BIN2
                    AND DATALENGTH(x.k)=DATALENGTH(y.k))) RETURN 0;
            INSERT INTO @todo SELECT x.v,y.v,x.t,y.t FROM @a AS x JOIN @b AS y
                ON x.k COLLATE Latin1_General_100_BIN2=y.k COLLATE Latin1_General_100_BIN2
                   AND DATALENGTH(x.k)=DATALENGTH(y.k);
        END;
        SET @index=@index+1;
    END;
    RETURN 1;
END;"""
    return KernelObject(logical, names.object(logical), "function", ddl)
