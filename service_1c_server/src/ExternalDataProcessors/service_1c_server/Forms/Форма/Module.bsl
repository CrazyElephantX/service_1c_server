Перем Cmd;
Перем Connect;

&НаКлиенте
Процедура Обслуживание(Команда)
	ОстановитьСлужбу(Сервер1с);
	ВыполнитьОбслуживаниеSQL(1);
	ЗапуститьСлужбу(Сервер1с);
	ВыполнитьОбслуживаниеSQL(2);
	ВыполнитьОбслуживаниеSQL(3);
КонецПроцедуры

&НаСервере
Процедура ВыполнитьОбслуживаниеSQL(шаг)
	ПодключитьSQL();
	ТекстSQL = ПолучитьТекстОбсуживания(шаг);
				
	Рез = ВыполнитьЗапрос(ТекстSQL);
	Если Рез = Неопределено Тогда 
		Возврат;         
	Иначе
		СообщениеОПроцессе("Выполнен шаг - " + шаг + Символы.ПС + "Результат: " + Рез, Истина);
	КонецЕсли;
КонецПроцедуры

Функция ПолучитьТекстОбсуживания(шаг)
	Если шаг = 1 Тогда 
		текст = "USE " + БазаСКУЛЬ + "; -- устанавливаем текущую базу
		|SET NOCOUNT ON; -- отключаем вывод количества возвращаемых строк, это несколько ускорит обработку
		|DECLARE @objectid int; -- ID объекта
		|DECLARE @indexid int; -- ID индекса
		|DECLARE @partitioncount bigint; -- количество секций если индекс секционирован
		|DECLARE @schemaname nvarchar(130); -- имя схемы в которой находится таблица
		|DECLARE @objectname nvarchar(130); -- имя таблицы 
		|DECLARE @indexname nvarchar(130); -- имя индекса
		|DECLARE @partitionnum bigint; -- номер секции
		|DECLARE @frag float; -- процент фрагментации индекса
		|DECLARE @command nvarchar(4000); -- инструкция T-SQL для дефрагментации либо ренидексации
		|
		|-- Отбор таблиц и индексов с помощью системного представления sys.dm_db_index_physical_stats
		|-- Отбор только тех объектов которые являются индексами (index_id > 0), 
		|-- фрагментация которых более 10% и количество страниц в индексе более 128
		|SELECT
		|    object_id AS objectid,
		|    index_id AS indexid,
		|    partition_number AS partitionnum,
		|    avg_fragmentation_in_percent AS frag
		|INTO #work_to_do
		|FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')
		|WHERE avg_fragmentation_in_percent > 10.0 AND index_id > 0 AND page_count > 128;
		|
		|-- Объявление курсора для чтения секций
		|DECLARE partitions CURSOR FOR SELECT * FROM #work_to_do;
		|
		|-- Открытие курсора
		|OPEN partitions;
		|
		|-- Цикл по секциям
		|WHILE (1=1)
		|    BEGIN;
		|        FETCH NEXT
		|           FROM partitions
		|           INTO @objectid, @indexid, @partitionnum, @frag;
		|        IF @@FETCH_STATUS < 0 BREAK;
		|		
		|-- Собираем имена объектов по ID		
		|        SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)
		|        FROM sys.objects AS o
		|        JOIN sys.schemas as s ON s.schema_id = o.schema_id
		|        WHERE o.object_id = @objectid;
		|        SELECT @indexname = QUOTENAME(name)
		|        FROM sys.indexes
		|        WHERE  object_id = @objectid AND index_id = @indexid;
		|        SELECT @partitioncount = count (*)
		|        FROM sys.partitions
		|        WHERE object_id = @objectid AND index_id = @indexid;
		|
		|-- Если фрагментация менее или равна 30% тогда дефрагментация, иначе реиндексация
		|        IF @frag <= 30.0
		|            SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REORGANIZE';
		|        IF @frag > 30.0
		|            SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REBUILD';
		|        IF @partitioncount > 1
		|            SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10));
		|			
		|-- Если реиндексация, то для ускорения добавляем параметры использования TEMPDB(имеет смысл только если TempDB на отдельном физ. диске) и многопроцессорной обработки 
		|			IF @frag > 30.0
		|			SET @command = @command + N' WITH (SORT_IN_TEMPDB = ON, MAXDOP = 0)';	
		|        EXEC (@command);
		|        PRINT N'Executed: ' + @command;
		|    END;
		|
		|-- Закрытие курсора
		|CLOSE partitions;
		|DEALLOCATE partitions;
		|
		|-- Удаление временной таблицы
		|DROP TABLE #work_to_do;
		|GO";
	ИначеЕсли Шаг = 2 Тогда
		текст = "USE " + БазаСКУЛЬ + ";
		|EXEC sp_updatestats;";
	ИначеЕсли Шаг = 3 Тогда
		текст = "USE " + ";
		|DBCC FREEPROCCACHE;";
	КонецЕсли;
	
	Возврат текст;
КонецФункции

Процедура СообщениеОПроцессе(ТекстСообщения, ВыводитьВремя = Ложь) 
	Сообщить(ТекстСообщения + " [" + ?(ВыводитьВремя, ТекущаяДата(),"") + "]");
КонецПроцедуры

Функция ВыполнитьЗапрос(ТекстSQL)
	Перем Строк;
	
	Попытка
		Cmd.CommandText = ТекстSQL;;
		//RS = Cmd.Execute(Строк);
		Cmd.Execute(Строк);
	Исключение
		СообщениеОПроцессе(ОписаниеОшибки() + Символы.ПС + "Запрос :   " + ТекстSQL);
		Возврат Неопределено;
	КонецПопытки;
		
	Возврат Строк;
	
КонецФункции

Процедура ПодключитьSQL()
	
	//СтруктураИБ = ПолучитьСтруктуруХраненияБазыДанных(, Истина);
	
	
	Если НЕ Соединение_ИБ() Тогда 
			СообщениеОПроцессе(ОписаниеОшибки());
			Возврат;
	КонецЕсли;
    Cmd = Новый COMОбъект("ADODB.Command");
	Cmd.ActiveConnection = Connect;
	Cmd.CommandTimeout = 0;

КонецПроцедуры

&НаСервере
Процедура ОстановитьСлужбу(Сервер)
	WinMGMT = ПолучитьCOMОбъект("winmgmts:\\" + Сервер + "\root\cimv2");
	Win32_Service = WinMGMT.ExecQuery("SELECT * FROM Win32_Service");
	
	Для Каждого Service ИЗ Win32_Service Цикл
		Если Service.Name = ИмяСлужбы1с Тогда
			Service.StopService();
			Прервать;
		КонецЕсли;
	КонецЦикла;
КонецПроцедуры

Функция Соединение_ИБ(ВремяОжидания=60) Экспорт  
	//to do: переписать функцию
	Если НЕ Connect = Неопределено Тогда 
		Возврат Истина;
	КонецЕсли;
	
	//СоединениеУстановлено = Ложь;

	Connect						= Новый COMОбъект("ADODB.Connection");
	Connect.ConnectionTimeOut	= ВремяОжидания;
	Connect.CursorLocation		= 3;
	
	СтрокаСоединенияИБ	= СтрокаСоединенияИнформационнойБазы();
	ПозицияИмяСервера	= Найти(СтрокаСоединенияИБ,"Srvr=""");
	Если ПозицияИмяСервера > 0 Тогда
		сСтрСоед		= Сред(СтрокаСоединенияИБ, ПозицияИмяСервера + 6);
		нКовычка		= Найти(сСтрСоед,"""");
		ИмяСервера		= Лев(сСтрСоед,нКовычка - 1);
		нДвоеточие		= Найти(ИмяСервера,":");
		Если нДвоеточие = 0 Тогда
			//Порт		= 1541;
		Иначе
			//Порт		= Сред(ИмяСервера,нДвоеточие + 1);
			ИмяСервера	= Лев(ИмяСервера,нДвоеточие - 1);
		КонецЕсли;
		сСтрСоед		= Сред(сСтрСоед,нКовычка + 1);
	Иначе
		Возврат Ложь;
	КонецЕсли;
	
	ПозицияИмяБД		= Найти(сСтрСоед,"Ref=""");
	Если ПозицияИмяБД > 0 Тогда
		сСтрСоед		= Сред(сСтрСоед,ПозицияИмяБД + 5);
		нКовычка		= Найти(сСтрСоед,"""");
		//ИмяБД			= Лев(сСтрСоед,нКовычка - 1);
		сСтрСоед		= Сред(сСтрСоед,нКовычка + 1);
	КонецЕсли;
	
	Попытка
		Connect.Open(СтрокаСоединения);
		Возврат Истина;
		
	Исключение
		Connect = Неопределено;
		Сообщить("Ошибка подключения к Sql
		|" + ОписаниеОшибки(),СтатусСообщения.ОченьВажное);
		Возврат Ложь;
	КонецПопытки;
	
КонецФункции

&НаСервере
Процедура ЗапуститьСлужбу(Сервер)
	WinMGMT = ПолучитьCOMОбъект("winmgmts:\\" + Сервер + "\root\cimv2");
	Win32_Service = WinMGMT.ExecQuery("SELECT * FROM Win32_Service");
	
	Для Каждого Service ИЗ Win32_Service Цикл
		Если Service.Name = ИмяСлужбы1с Тогда
			Service.StartService();
			Прервать;
		КонецЕсли;
	КонецЦикла;
КонецПроцедуры

&НаСервере
Процедура ПриСозданииНаСервере(Отказ, СтандартнаяОбработка)
	СерверСКУЛЬ = "SQL-1с-Server01";
	БазаСКУЛЬ = "SQL_base";
	Сервер1с = "SQL-1с-Server01";
	ИмяСлужбы1с = "1C:Enterprise 8.3 Server Agent (x86-64)";
	СобратьСтрокуСоединения();
КонецПроцедуры

&НаКлиенте
Процедура СерверСКУЛЬПриИзменении(Элемент)
	СобратьСтрокуСоединения();
КонецПроцедуры


&НаСервере
Процедура СобратьСтрокуСоединения()
	СтрокаСоединения = "Provider=SQLOLEDB.1;Integrated Security = SSPI;Initial Catalog=" + СерверСКУЛЬ + ";Data Source=" + БазаСКУЛЬ;
КонецПроцедуры

&НаКлиенте
Процедура БазаСКУЛЬПриИзменении(Элемент)
	СобратьСтрокуСоединения();
КонецПроцедуры


