DELIMITER $$

USE `operator_scanners`$$

DROP PROCEDURE IF EXISTS `createFullDelta`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `createFullDelta`(scan_table1 VARCHAR(64), scan_table2 VARCHAR(64), binn SMALLINT(4))
BEGIN

SET @temp_grid = CONCAT('tmpGrd_',scan_table1);
SET @gridExist = 'FALSE';
SET @bin = binn;

# GSM raw files RxLev delta calculation

SET @j=	CONCAT('SET @g = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_gsm\')');
PREPARE stmt FROM @j;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @k=	CONCAT('SET @h = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_gsm\')');
PREPARE stmt FROM @k;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF (@g IS NOT NULL AND @h IS NOT NULL) THEN 
	
	SET @gsmTable=CONCAT(scan_table1,'_gsm');
	SET @gsmTable2=CONCAT(scan_table2,'_gsm');
	SET @TableDelta = CONCAT(scan_table1,'_delta_',binn);
	SET @gsmTableDelta=CONCAT(scan_table1,'_delta_',binn,'_gsm');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@gsmTableDelta);
	
	PREPARE stmt1 FROM @t1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
	SET @t2=CONCAT(
		'CREATE TABLE ',@gsmTableDelta,' (Lon DOUBLE, Lat DOUBLE, ARFCN INTEGER(10), BSIC INTEGER(10), ScanRxLev_Delta_dB DOUBLE, 
						INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			');
			
	PREPARE stmt2 FROM @t2;
	EXECUTE stmt2;
	DEALLOCATE PREPARE stmt2;

	# Create temporary main tables with full DT 

	DROP TABLE IF EXISTS tempMain2g;
	DROP TABLE IF EXISTS tempMain2g_2;
	
	SET @m1=CONCAT(
		'create temporary table tempMain2g ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `ARFCN` INTEGER(10), `BSIC` INTEGER(10), `Scanned_level_dBm` DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@gsmTable, 
			' WHERE (ARFCN >= 0 AND ARFCN <= 26) OR (ARFCN >= 995 AND ARFCN <= 1024) OR (ARFCN >= 662 AND ARFCN <= 735)');
	
	PREPARE stmt FROM @m1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @m2=CONCAT(
		'create temporary table tempMain2g_2 ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `ARFCN` INTEGER(10), `BSIC` INTEGER(10), `Scanned_level_dBm` DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@gsmTable2, 
			' WHERE (ARFCN >= 0 AND ARFCN <= 26) OR (ARFCN >= 995 AND ARFCN <= 1024) OR (ARFCN >= 662 AND ARFCN <= 735)');
	
	PREPARE stmt FROM @m2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# Find Min,Max of Lat and Lon from DT
	
	SET @latMinDT = (SELECT MIN(lat) FROM tempMain2g);
	SET @lonMinDT = (SELECT MIN(lon) FROM tempMain2g);
	SET @latMaxDT = (SELECT MAX(lat) FROM tempMain2g);
	SET @lonMaxDT = (SELECT MAX(lon) FROM tempMain2g);
	
	# Set Min and Max location of DT with offset 1 binn
	
	SET @locMinDT = GEOMFROMTEXT(CONCAT('POINT(',@latMinDT,' ',@lonMinDT,')'));
	SET @locMaxDT = GEOMFROMTEXT(CONCAT('POINT(',@latMaxDT,' ',@lonMaxDT,')'));
	
		
	SET @latMaxDToffset = X(dLocationGeom(@locMaxDT,0,binn));
	SET @lonMaxDToffset = Y(dLocationGeom(@locMaxDT,90,binn));
	
	# create the grid
		
	SET @locStart = @locMinDT;
	SET @latStart = @latMinDT;
	SET @lonStart = @lonMinDT;
	
	SET @grd1 = CONCAT('DROP TABLE IF EXISTS ',@temp_grid);
		
	PREPARE stmt FROM @grd1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @grd2 = CONCAT('
	CREATE TABLE ',@temp_grid,' (id INT NOT NULL AUTO_INCREMENT, swPoint GEOMETRY NOT NULL, nePoint GEOMETRY NOT NULL, 
					polygons POLYGON NOT NULL, binCount_Lat INTEGER, binCount_Lon INTEGER, PRIMARY KEY (id), 
					SPATIAL INDEX (polygons)) ENGINE = MYISAM
			');
		
	PREPARE stmt FROM @grd2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
	# find the points for the polygon bin
	
	SET @i=1;	
	WHILE @latStart <= @latMaxDToffset
		DO
		SET @j=1;
		WHILE @lonStart <= @lonMaxDToffset
			DO
			SET @locA = @locStart;
			SET @locB = dLocationGeom(@locA,90,binn);
			SET @locC = dLocationGeom(@locB,0,binn);
			SET @locD = dLocationGeom(@locC,270,binn);
			
			# set the next Latitude start
			
			IF @j=1 THEN SET @locStartNextLat = @locD;
			END IF;
			
			# create and insert the polygon bin into the grid table
			
			SET @poly=POLYGONFROMTEXT(CONCAT('POLYGON((',X(@locA),' ', Y(@locA),',',X(@locB),' ',Y(@locB),',',X(@locC),' ',Y(@locC),',',X(@locD),' ',Y(@locD),',',X(@locA),' ',Y(@locA),'))'));
			
			SET @grd3=CONCAT('
			INSERT INTO ',@temp_grid,' (swPoint, nePoint, polygons, binCount_Lat, binCount_Lon) VALUES (@locA, @locC, @poly, @i, @j)
			');
			
			PREPARE stmt FROM @grd3;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;

			SET @locStart=@locB;
			SET @lonStart=Y(@locStart);
			SET @j=@j+1;
		END WHILE;
		
		#set @locStart = dLocationGeom(@locMinDT,0,binn*@i);
		SET @locStart = @locStartNextLat;
		SET @latStart = X(@locStart);
		SET @lonStart = Y(@locStart);
		SET @i=@i+1;
		
	END WHILE;
	
	SET @gridExist = 'TRUE';
	
	
	# find the points inside each polygon bin and take the average RxLev values for each ARFCN&BSIC and insert them into final binned tables

	DROP TABLE IF EXISTS grd_preData;
	DROP TABLE IF EXISTS grd_postData;
	
	SET @d1=CONCAT(
		'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`ARFCN`,a.`BSIC`, ROUND(AVG(a.`Scanned_level_dBm`),1) AS Scanned_level_dBm 
			FROM `tempMain2g` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`ARFCN`,a.`BSIC`'
			);
	
	PREPARE stmt FROM @d1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	

	SET @d2=CONCAT(
		'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`ARFCN`,a.`BSIC`, ROUND(AVG(a.`Scanned_level_dBm`),1) AS Scanned_level_dBm 
			FROM `tempMain2g_2` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`ARFCN`,a.`BSIC`'
			);
	
	PREPARE stmt FROM @d2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
	# find the delta between RxLev of the equally bins from PRE and POST tables
	SET @t4=CONCAT(
		'INSERT INTO ',@gsmTableDelta,' (Lon, Lat, ARFCN, BSIC, ScanRxLev_Delta_dB)
			SELECT a.Lon, a.Lat, a.ARFCN, a.BSIC, ROUND(AVG(-1*(a.`Scanned_level_dBm`-b.`Scanned_level_dBm`)),1)
			FROM grd_preData a 
				JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
			WHERE a.ARFCN = b.ARFCN AND a.BSIC = b.BSIC
			GROUP BY Lon, Lat, ARFCN, BSIC'
			);
	
	PREPARE stmt4 FROM @t4;
	EXECUTE stmt4;
	DEALLOCATE PREPARE stmt4;	
END IF;	

# GSM best server delta calculation

SET @j=	CONCAT('SET @g = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_gsm_bestserver\')');
PREPARE stmt FROM @j;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @k=	CONCAT('SET @h = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_gsm_bestserver\')');
PREPARE stmt FROM @k;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF (@g IS NOT NULL AND @h IS NOT NULL) THEN 
	
	SET @gsmBestTable=CONCAT(scan_table1,'_gsm_bestserver');
	SET @gsmBestTable2=CONCAT(scan_table2,'_gsm_bestserver');
	SET @TableBestDelta = CONCAT(scan_table1,'_best_delta_',binn);
	SET @gsmBestTableDelta = CONCAT(scan_table1,'_best_delta_',binn,'_gsm');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@gsmBestTableDelta);
	
	PREPARE stmt1 FROM @t1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
	SET @t2=CONCAT(
		'CREATE TABLE ',@gsmBestTableDelta,' (Lon DOUBLE, Lat DOUBLE, ScanRxLev_Delta_dB DOUBLE,
							INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			');
			
	PREPARE stmt2 FROM @t2;
	EXECUTE stmt2;
	DEALLOCATE PREPARE stmt2;


	# Create temporary best tables with full DT 

	DROP TABLE IF EXISTS bestMain2g;
	DROP TABLE IF EXISTS bestMain2g_2;
	
	SET @m1=CONCAT(
		'create temporary table bestMain2g ( 
				`GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `ARFCN` INTEGER(10), `BSIC` INTEGER(10), 
				`Best_Scanned_Level` DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@gsmBestTable);
	
	PREPARE stmt FROM @m1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @m2=CONCAT(
		'create temporary table bestMain2g_2 ( 
				`GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `ARFCN` INTEGER(10), `BSIC` INTEGER(10), 
				`Best_Scanned_Level` DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@gsmBestTable2);
	
	PREPARE stmt FROM @m2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	
	# find the points inside each polygon bin and take the average of best RxLev values and insert them into final binned table

	DROP TABLE IF EXISTS grd_preData;
	DROP TABLE IF EXISTS grd_postData;
	
	SET @b1=CONCAT(
		'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, ROUND(AVG(a.`Best_Scanned_Level`),1) AS Scanned_level_dBm
			FROM `tempMain2g` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons'
			);
	
	PREPARE stmt FROM @d1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	

	SET @b2=CONCAT(
		'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, ROUND(AVG(a.`Best_Scanned_Level`),1) AS Scanned_level_dBm
			FROM `tempMain2g_2` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons'
			);
	
	PREPARE stmt FROM @d2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
	# find the delta between RxLev of the equally bins from PRE and POST tables
	SET @t4=CONCAT(
		'INSERT INTO ',@gsmBestTableDelta,' (Lon, Lat, ScanRxLev_Delta_dB)
			SELECT a.Lon, a.Lat, ROUND(AVG(-1*(a.`Scanned_level_dBm`-b.`Scanned_level_dBm`)),1)
			FROM grd_preData a 
				JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
			GROUP BY Lon, Lat'
			);
	
	PREPARE stmt4 FROM @t4;
	EXECUTE stmt4;
	DEALLOCATE PREPARE stmt4;	
END IF;	
		
# UMTS EcIo and RSCP delta calculation

SET @j=	CONCAT('SET @u = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_umts\')');

PREPARE stmt FROM @j;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @k=	CONCAT('SET @v = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_umts\')');

PREPARE stmt FROM @k;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


IF (@u IS NOT NULL AND @v IS NOT NULL) THEN 
	SET @umtsTable=CONCAT(scan_table1,'_umts');
	SET @umtsTable2=CONCAT(scan_table2,'_umts');
	SET @umtsTableDelta=CONCAT(scan_table1,'_delta_',binn,'_umts');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@umtsTableDelta);
	
	PREPARE stmt FROM @t1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @t2=CONCAT(
		'CREATE TABLE ',@umtsTableDelta,' (Lon DOUBLE, Lat DOUBLE, UARFCN INTEGER(10), PSC INTEGER(10), EcIo_Delta_dB DOUBLE, RSCP_Delta_dB DOUBLE, 
						INDEX Lon (`Lon`), INDEX Lat (`Lat`))
		');
	PREPARE stmt FROM @t2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	 
	# Create temporary main tables with full DT 

	DROP TABLE IF EXISTS tempMain3g;
	DROP TABLE IF EXISTS tempMain3g_2;
	
	SET @m1=CONCAT(
		'create temporary table tempMain3g ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `UARFCN` INTEGER(10), `PSC` INTEGER(10), `EcIo_dB` DOUBLE, RSCP_dBm DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@umtsTable, 
			' WHERE UARFCN IN (10762,10787,10812,10837,2981,2977,2958)');
	
	PREPARE stmt FROM @m1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @m2=CONCAT(
		'create temporary table tempMain3g_2 ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `UARFCN` INTEGER(10), `PSC` INTEGER(10), `EcIo_dB` DOUBLE, RSCP_dBm DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@umtsTable2, 
			' WHERE UARFCN IN (10762,10787,10812,10837,2981,2977,2958)');
	
	PREPARE stmt FROM @m2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# Check if grid table for this DT was already created
	
	IF @gridExist = 'FALSE' 
		THEN
		
		# if grid table not exist - create the grid table
		# Find Min,Max of Lat and Lon from DT
		
		SET @latMinDT = (SELECT MIN(lat) FROM tempMain3g);
		SET @lonMinDT = (SELECT MIN(lon) FROM tempMain3g);
		SET @latMaxDT = (SELECT MAX(lat) FROM tempMain3g);
		SET @lonMaxDT = (SELECT MAX(lon) FROM tempMain3g);
		
		# Set Min and Max location of DT with offset 1 binn
		
		SET @locMinDT = GEOMFROMTEXT(CONCAT('POINT(',@latMinDT,' ',@lonMinDT,')'));
		SET @locMaxDT = GEOMFROMTEXT(CONCAT('POINT(',@latMaxDT,' ',@lonMaxDT,')'));
		
			
		SET @latMaxDToffset = X(dLocationGeom(@locMaxDT,0,binn));
		SET @lonMaxDToffset = Y(dLocationGeom(@locMaxDT,90,binn));
		
		# create the grid
			
		SET @locStart = @locMinDT;
		SET @latStart = @latMinDT;
		SET @lonStart = @lonMinDT;
		
		#drop table if exists temp_grid;
		SET @grd1 = CONCAT('DROP TABLE IF EXISTS ',@temp_grid);
			
		PREPARE stmt FROM @grd1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @grd2 = CONCAT('
		CREATE TABLE ',@temp_grid,' (id INT NOT NULL AUTO_INCREMENT, swPoint GEOMETRY NOT NULL, nePoint GEOMETRY NOT NULL, 
						polygons POLYGON NOT NULL, binCount_Lat INTEGER, binCount_Lon INTEGER, PRIMARY KEY (id), 
						SPATIAL INDEX (polygons)) ENGINE = MYISAM
				');
			
		PREPARE stmt FROM @grd2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;	
		
		# find the points for the polygon bin
		
		SET @i=1;	
		WHILE @latStart <= @latMaxDToffset
			DO
			SET @j=1;
			WHILE @lonStart <= @lonMaxDToffset
				DO
				SET @locA = @locStart;
				SET @locB = dLocationGeom(@locA,90,binn);
				SET @locC = dLocationGeom(@locB,0,binn);
				SET @locD = dLocationGeom(@locC,270,binn);
				
				# set the next Latitude start
				
				IF @j=1 THEN SET @locStartNextLat = @locD;
				END IF;
				
				# create and insert the polygon bin into the grid table
				
				SET @poly=POLYGONFROMTEXT(CONCAT('POLYGON((',X(@locA),' ', Y(@locA),',',X(@locB),' ',Y(@locB),',',X(@locC),' ',Y(@locC),',',X(@locD),' ',Y(@locD),',',X(@locA),' ',Y(@locA),'))'));
				
				SET @grd3=CONCAT('
				INSERT INTO ',@temp_grid,' (swPoint, nePoint, polygons, binCount_Lat, binCount_Lon) VALUES (@locA, @locC, @poly, @i, @j)
				');
				
				PREPARE stmt FROM @grd3;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;

				SET @locStart=@locB;
				SET @lonStart=Y(@locStart);
				SET @j=@j+1;
			END WHILE;
			
			#set @locStart = dLocationGeom(@locMinDT,0,binn*@i);
			SET @locStart = @locStartNextLat;
			SET @latStart = X(@locStart);
			SET @lonStart = Y(@locStart);
			SET @i=@i+1;
			
		END WHILE;
		
		SET @gridExist = 'TRUE';
		
	END IF;
	
	# find the points inside each polygon bin and take the average RSCP and EcIo values for each PSC and insert them into final binned tables
	
	DROP TABLE IF EXISTS grd_preData;
	DROP TABLE IF EXISTS grd_postData;
	
	SET @d1=CONCAT(
		'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`UARFCN`,a.`PSC`, ROUND(AVG(a.`EcIo_dB`),1) AS EcIo_dB,  
				ROUND(AVG(RSCP_dBm),1) AS RSCP_dBm
			FROM `tempMain3g` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`UARFCN`,a.`PSC`'
			);
	
	PREPARE stmt FROM @d1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @d2=CONCAT(
		'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`UARFCN`,a.`PSC`, ROUND(AVG(a.`EcIo_dB`),1) AS EcIo_dB,  
				ROUND(AVG(RSCP_dBm),1) AS RSCP_dBm
			FROM `tempMain3g_2` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`UARFCN`,a.`PSC`'
			);
	
	PREPARE stmt FROM @d2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# find the delta between EcIo and RSCP of the equally bins from PRE and POST tables
	
	SET @t4=CONCAT(
		'INSERT INTO ',@umtsTableDelta,' (Lon, Lat, UARFCN, PSC, EcIo_Delta_dB, RSCP_Delta_dB)
			SELECT 	a.Lon, a.Lat, a.`UARFCN`,a.`PSC`,
				ROUND(AVG(-1*(a.`EcIo_dB`-b.`EcIo_dB`)),1),
				ROUND(AVG(-1*(a.`RSCP_dBm`-b.`RSCP_dBm`)),1)
			FROM grd_preData a 
				JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
			WHERE a.UARFCN = b.UARFCN AND a.PSC = b.PSC
			GROUP BY Lon, Lat, UARFCN, PSC'
			);
			
	PREPARE stmt4 FROM @t4;
	EXECUTE stmt4;
	DEALLOCATE PREPARE stmt4;
		
END IF;	

# UMTS best EcIo and RSCP delta calculation

SET @vivaFreqs3G = '10762,10787,10812,10837,2981,2977,2958,';

WHILE (LOCATE(',', @vivaFreqs3G) > 0) 
DO

	SET @freq3G = LEFT(@vivaFreqs3G, LOCATE(',',@vivaFreqs3G)-1);
	SET @vivaFreqs3G = SUBSTRING(@vivaFreqs3G, LOCATE(',',@vivaFreqs3G) + 1);
	
	SET @j=	CONCAT('SET @u = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_umts_bestserver_', @freq3G,'\')');

	PREPARE stmt FROM @j;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @k=	CONCAT('SET @v = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_umts_bestserver_', @freq3G,'\')');

	PREPARE stmt FROM @k;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;


	IF (@u IS NOT NULL AND @v IS NOT NULL) THEN 
		SET @umtsBestTable=CONCAT(scan_table1,'_umts_bestserver_',@freq3G);
		SET @umtsBestTable2=CONCAT(scan_table2,'_umts_bestserver_',@freq3G);
		SET @umtsBestTableDelta=CONCAT(scan_table1,'_best_delta_',binn,'_',@freq3G);
		
		SET @t1=CONCAT('DROP TABLE IF EXISTS ',@umtsBestTableDelta);
		
		PREPARE stmt FROM @t1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @t2=CONCAT(
			'CREATE TABLE ',@umtsBestTableDelta,' (Lon DOUBLE, Lat DOUBLE, UARFCN INTEGER(10), EcIo_Delta_dB DOUBLE, RSCP_Delta_dB DOUBLE, 
							INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			');
		PREPARE stmt FROM @t2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		 
		# Create temporary best tables with full DT 

		DROP TABLE IF EXISTS tempMain3g;
		DROP TABLE IF EXISTS tempMain3g_2;
		
		SET @m1=CONCAT(
			'create temporary table tempMain3g ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `UARFCN` INTEGER(10), `PSC` INTEGER(10), `EcIo_dB` DOUBLE, RSCP_dBm DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@umtsBestTable);
		
		PREPARE stmt FROM @m1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @m2=CONCAT(
			'create temporary table tempMain3g_2 ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), `UARFCN` INTEGER(10), `PSC` INTEGER(10), `EcIo_dB` DOUBLE, RSCP_dBm DOUBLE, locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@umtsBestTable2);
		
		PREPARE stmt FROM @m2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		# find the points inside each polygon bin and take the average of best RSCP and EcIo values and insert them into final binned tables
	
		DROP TABLE IF EXISTS grd_preData;
		DROP TABLE IF EXISTS grd_postData;
	
		SET @d1=CONCAT(
			'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`UARFCN`, ROUND(AVG(a.`EcIo_dB`),1) AS EcIo_dB,  
					ROUND(AVG(RSCP_dBm),1) AS RSCP_dBm
				FROM `tempMain3g` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons, a.`UARFCN`'
				);
	
		PREPARE stmt FROM @d1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @d2=CONCAT(
			'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`UARFCN`, ROUND(AVG(a.`EcIo_dB`),1) AS EcIo_dB,  
					ROUND(AVG(RSCP_dBm),1) AS RSCP_dBm
				FROM `tempMain3g_2` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons, a.`UARFCN`'
				);
		
		PREPARE stmt FROM @d2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		# find the delta between EcIo and RSCP of the equally bins from PRE and POST tables
		
		SET @t4=CONCAT(
			'INSERT INTO ',@umtsBestTableDelta,' (Lon, Lat, UARFCN, EcIo_Delta_dB, RSCP_Delta_dB)
				SELECT 	a.Lon, a.Lat, a.`UARFCN`,
					ROUND(AVG(-1*(a.`EcIo_dB`-b.`EcIo_dB`)),1),
					ROUND(AVG(-1*(a.`RSCP_dBm`-b.`RSCP_dBm`)),1)
				FROM grd_preData a 
					JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
				WHERE a.UARFCN = b.UARFCN
				GROUP BY Lon, Lat, UARFCN'
				);
				
		PREPARE stmt4 FROM @t4;
		EXECUTE stmt4;
		DEALLOCATE PREPARE stmt4;
	END IF;
END WHILE;
	
# LTE CINR, RSRQ and RSRP calculation

SET @j=	CONCAT('SET @l = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_lte\')');

PREPARE stmt FROM @j;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @k=	CONCAT('SET @m = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_lte\')');

PREPARE stmt FROM @k;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF (@l IS NOT NULL AND @m IS NOT NULL) THEN 
	SET @lteTable=CONCAT(scan_table1,'_lte');
	SET @lteTable2=CONCAT(scan_table2,'_lte');
	SET @lteTableDelta=CONCAT(scan_table1,'_delta_',binn,'_lte');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@lteTableDelta);
	
	PREPARE stmt FROM @t1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @t2=CONCAT(
		'CREATE TABLE ',@lteTableDelta,' (Lon DOUBLE, Lat DOUBLE, EARFCN INTEGER(10), PCI INTEGER(10), RSRP_Delta_dB DOUBLE, RSRQ_Delta_dB DOUBLE, CINR_Delta_dB DOUBLE,
						INDEX Lon (`Lon`), INDEX Lat (`Lat`))
		');
	PREPARE stmt FROM @t2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# Create temporary main table with full DT 

	DROP TABLE IF EXISTS tempMain4g;
	DROP TABLE IF EXISTS tempMain4g_2;
	
	SET @m1=CONCAT(
		'create temporary table tempMain4g ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), 
			`EARFCN` INTEGER(10), `DL Bandwidth` INTEGER(10),`PCI` INTEGER(10), `RSRP` DOUBLE, `RSRQ` DOUBLE, `CINR` DOUBLE,
			locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@lteTable, 
			' WHERE EARFCN IN (3150,1575,3516,575,550,525)');
	
	PREPARE stmt FROM @m1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @m2=CONCAT(
		'create temporary table tempMain4g_2 ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), 
			`EARFCN` INTEGER(10), `DL Bandwidth` INTEGER(10),`PCI` INTEGER(10), `RSRP` DOUBLE, `RSRQ` DOUBLE, `CINR` DOUBLE,
			locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
			select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@lteTable2, 
			' WHERE EARFCN IN (3150,1575,3516,575,550,525)');
	
	PREPARE stmt FROM @m2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
		# Check if grid table for this DT was already created
	
	IF @gridExist <> 'TRUE' 
		THEN
		
		# if grid table not exist - create the grid table
		# Find Min,Max of Lat and Lon from DT
		
		SET @latMinDT = (SELECT MIN(lat) FROM tempMain4g);
		SET @lonMinDT = (SELECT MIN(lon) FROM tempMain4g);
		SET @latMaxDT = (SELECT MAX(lat) FROM tempMain4g);
		SET @lonMaxDT = (SELECT MAX(lon) FROM tempMain4g);
		
		# Set Min and Max location of DT with offset 1 binn
		
		SET @locMinDT = GEOMFROMTEXT(CONCAT('POINT(',@latMinDT,' ',@lonMinDT,')'));
		SET @locMaxDT = GEOMFROMTEXT(CONCAT('POINT(',@latMaxDT,' ',@lonMaxDT,')'));
		
			
		SET @latMaxDToffset = X(dLocationGeom(@locMaxDT,0,binn));
		SET @lonMaxDToffset = Y(dLocationGeom(@locMaxDT,90,binn));
		
		# create the grid
			
		SET @locStart = @locMinDT;
		SET @latStart = @latMinDT;
		SET @lonStart = @lonMinDT;
		
		#drop table if exists temp_grid;
		SET @grd1 = CONCAT('DROP TABLE IF EXISTS ',@temp_grid);
			
		PREPARE stmt FROM @grd1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @grd2 = CONCAT('
		CREATE TABLE ',@temp_grid,' (id INT NOT NULL AUTO_INCREMENT, swPoint GEOMETRY NOT NULL, nePoint GEOMETRY NOT NULL, 
						polygons POLYGON NOT NULL, binCount_Lat INTEGER, binCount_Lon INTEGER, PRIMARY KEY (id),
						SPATIAL INDEX (polygons)) ENGINE = MYISAM
				');
			
		PREPARE stmt FROM @grd2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;	
		
		# find the points for the polygon bin
		
		SET @i=1;	
		WHILE @latStart <= @latMaxDToffset
			DO
			SET @j=1;
			WHILE @lonStart <= @lonMaxDToffset
				DO
				SET @locA = @locStart;
				SET @locB = dLocationGeom(@locA,90,binn);
				SET @locC = dLocationGeom(@locB,0,binn);
				SET @locD = dLocationGeom(@locC,270,binn);
				
				# set the next Latitude start
				
				IF @j=1 THEN SET @locStartNextLat = @locD;
				END IF;
				
				# create and insert the polygon bin into the grid table
				
				SET @poly=POLYGONFROMTEXT(CONCAT('POLYGON((',X(@locA),' ', Y(@locA),',',X(@locB),' ',Y(@locB),',',X(@locC),' ',Y(@locC),',',X(@locD),' ',Y(@locD),',',X(@locA),' ',Y(@locA),'))'));
				
				SET @grd3=CONCAT('
				INSERT INTO ',@temp_grid,' (swPoint, nePoint, polygons, binCount_Lat, binCount_Lon) VALUES (@locA, @locC, @poly, @i, @j)
				');
				
				PREPARE stmt FROM @grd3;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;

				SET @locStart=@locB;
				SET @lonStart=Y(@locStart);
				SET @j=@j+1;
			END WHILE;
			
			#set @locStart = dLocationGeom(@locMinDT,0,binn*@i);
			SET @locStart = @locStartNextLat;
			SET @latStart = X(@locStart);
			SET @lonStart = Y(@locStart);
			SET @i=@i+1;
			
		END WHILE;
		
		SET @gridExist = 'TRUE';
		
	END IF;	
	
	# find the points inside each polygon bin and take the average RSRP, RSRQ, CINR and DL_Bandwidth values for each PCI and insert them into final binned tables
		
	DROP TABLE IF EXISTS grd_preData;
	DROP TABLE IF EXISTS grd_postData;
	
	SET @d1=CONCAT(
		'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`EARFCN`,a.`PCI`, ROUND(AVG(a.`RSRP`),1) AS RSRP,  
				ROUND(AVG(RSRQ),1) AS RSRQ, ROUND(AVG(CINR),1) AS CINR
			FROM `tempMain4g` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`EARFCN`,a.`PCI`'
			);
	
	PREPARE stmt FROM @d1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @d2=CONCAT(
		'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`EARFCN`,a.`PCI`, ROUND(AVG(a.`RSRP`),1) AS RSRP,  
				ROUND(AVG(RSRQ),1) AS RSRQ, ROUND(AVG(CINR),1) AS CINR
			FROM `tempMain4g_2` a, ',@temp_grid,' grd
			WHERE CONTAINS(grd.polygons,a.`locPoint`)
			GROUP BY grd.polygons, a.`EARFCN`,a.`PCI`'
			);
	
	PREPARE stmt FROM @d2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# find the delta between EcIo and RSCP of the equally bins from PRE and POST tables
	
	SET @t4=CONCAT(
		'INSERT INTO ',@lteTableDelta,' (Lon, Lat, EARFCN, PCI, RSRP_Delta_dB, RSRQ_Delta_dB, CINR_Delta_dB)
			SELECT 	a.Lon, a.Lat, a.`EARFCN`,a.`PCI`,
				ROUND(AVG(-1*(a.`RSRP`-b.`RSRP`)),1),
				ROUND(AVG(-1*(a.`RSRQ`-b.`RSRQ`)),1),
				CASE WHEN (LEFT(a.`CINR`,1) = \'-\' OR LEFT(b.`CINR`,1) = \'-\') 
					THEN ROUND(AVG(-1*(a.`CINR`-b.`CINR`)),1)
					ELSE ROUND(AVG((b.`CINR`-a.`CINR`)),1)
				END
			FROM grd_preData a 
				JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
			WHERE a.EARFCN = b.EARFCN AND a.PCI = b.PCI
			GROUP BY Lon, Lat, EARFCN, PCI'
			);
	
	PREPARE stmt4 FROM @t4;
	EXECUTE stmt4;
	DEALLOCATE PREPARE stmt4;
		
END IF;	

# LTE best CINR, RSRQ and RSRP calculation

SET @vivaFreqs4G = '3150,1575,3516,575,550,525,';

WHILE (LOCATE(',', @vivaFreqs4G) > 0) 
DO
	SET @freq4G = LEFT(@vivaFreqs4G, LOCATE(',',@vivaFreqs4G)-1);
	SET @vivaFreqs4G = SUBSTRING(@vivaFreqs4G, LOCATE(',',@vivaFreqs4G) + 1);

	SET @j=	CONCAT('SET @l = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_lte_bestserver_', @freq4G, '\')');

	PREPARE stmt FROM @j;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @k=	CONCAT('SET @m = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_lte_bestserver_', @freq4G, '\')');

	PREPARE stmt FROM @k;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	IF (@l IS NOT NULL AND @m IS NOT NULL) THEN 
		SET @lteBestTable=CONCAT(scan_table1,'_lte_bestserver_',@freq4G );
		SET @lteBestTable2=CONCAT(scan_table2,'_lte_bestserver_',@freq4G );
		SET @lteBestTableDelta=CONCAT(scan_table1,'_best_delta_',binn,'_',@freq4G );
		
		SET @t1=CONCAT('DROP TABLE IF EXISTS ',@lteBestTableDelta);
		
		PREPARE stmt FROM @t1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @t2=CONCAT(
			'CREATE TABLE ',@lteBestTableDelta,' 
				(Lon DOUBLE, Lat DOUBLE, EARFCN INTEGER(10), RSRP_Delta_dB DOUBLE, RSRQ_Delta_dB DOUBLE, CINR_Delta_dB DOUBLE,
							INDEX Lon (`Lon`), INDEX Lat (`Lat`))
			');
		PREPARE stmt FROM @t2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		# Create temporary main table with full DT 

		DROP TABLE IF EXISTS tempMain4g;
		DROP TABLE IF EXISTS tempMain4g_2;
		
		SET @m1=CONCAT(
			'create temporary table tempMain4g ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), 
				`EARFCN` INTEGER(10), `RSRP` DOUBLE, `RSRQ` DOUBLE, `CINR` DOUBLE,
				locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@lteBestTable);
		
		PREPARE stmt FROM @m1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @m2=CONCAT(
			'create temporary table tempMain4g_2 ( `GPS_Time` VARCHAR(20), `Lon` DOUBLE, `Lat` DOUBLE, `Time`  VARCHAR(20), 
				`EARFCN` INTEGER(10), `DL Bandwidth` INTEGER(10),`PCI` INTEGER(10), `RSRP` DOUBLE, `RSRQ` DOUBLE, `CINR` DOUBLE,
				locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@lteBestTable2);
		
		PREPARE stmt FROM @m2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		# find the points inside each polygon bin and take the average RSRP, RSRQ, CINR and DL_Bandwidth values for each PCI and insert them into final binned tables
		
		DROP TABLE IF EXISTS grd_preData;
		DROP TABLE IF EXISTS grd_postData;
		
		SET @d1=CONCAT(
			'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`EARFCN`, ROUND(AVG(a.`RSRP`),1) AS RSRP,  
					ROUND(AVG(RSRQ),1) AS RSRQ, ROUND(AVG(CINR),1) AS CINR
				FROM `tempMain4g` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons, a.`EARFCN`'
				);
		
		PREPARE stmt FROM @d1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @d2=CONCAT(
			'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, a.`EARFCN`, ROUND(AVG(a.`RSRP`),1) AS RSRP,  
					ROUND(AVG(RSRQ),1) AS RSRQ, ROUND(AVG(CINR),1) AS CINR
				FROM `tempMain4g_2` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons, a.`EARFCN`'
				);
		
		PREPARE stmt FROM @d2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		# find the delta between EcIo and RSCP of the equally bins from PRE and POST tables
		
		SET @t4=CONCAT(
			'INSERT INTO ',@lteBestTableDelta,' (Lon, Lat, EARFCN, RSRP_Delta_dB, RSRQ_Delta_dB, CINR_Delta_dB)
				SELECT 	a.Lon, a.Lat, a.`EARFCN`,
					ROUND(AVG(-1*(a.`RSRP`-b.`RSRP`)),1),
					ROUND(AVG(-1*(a.`RSRQ`-b.`RSRQ`)),1),
					CASE WHEN (LEFT(a.`CINR`,1) = \'-\' OR LEFT(b.`CINR`,1) = \'-\') 
						THEN ROUND(AVG(-1*(a.`CINR`-b.`CINR`)),1)
						ELSE ROUND(AVG((b.`CINR`-a.`CINR`)),1)
					END
				FROM grd_preData a 
					JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
				WHERE a.EARFCN = b.EARFCN
				GROUP BY Lon, Lat, EARFCN'
				);
		
		PREPARE stmt4 FROM @t4;
		EXECUTE stmt4;
		DEALLOCATE PREPARE stmt4;

	END IF;
END WHILE;

#Find ue tables for delta calculation
	
DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table1, '_ue_%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables 
		WHERE table_name LIKE @searchName AND table_name NOT LIKE '%delta%' AND table_name NOT LIKE '%lb';
	
SET @ues = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @ue = (SELECT SUBSTRING_INDEX(`names`, '_', -1) FROM tableNames WHERE id=@j);
	SET @ues = (CONCAT(@ue,',',@ues));
	SET @j = @j+1;
END WHILE;


WHILE (LOCATE(',', @ues) > 0) 
DO
	SET @ue = LEFT(@ues, LOCATE(',',@ues)-1);
	SET @ues = SUBSTRING(@ues, LOCATE(',',@ues) + 1);

	SET @j=	CONCAT('SET @l = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table1,'_ue_', @ue, '\')');

	PREPARE stmt FROM @j;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @k=	CONCAT('SET @m = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table2,'_ue_', @ue, '\')');

	PREPARE stmt FROM @k;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	IF (@l IS NOT NULL AND @m IS NOT NULL) THEN 
		SET @ueTable=CONCAT('operator_ues.',scan_table1,'_ue_',@ue );
		SET @ueTable2=CONCAT('operator_ues.',scan_table2,'_ue_',@ue );
		SET @ueTableDelta=CONCAT('operator_ues.',scan_table1,'_delta_',binn,'_ue',@ue );
		
		# Create temporary main table with UE data 

		DROP TABLE IF EXISTS tempMain;
		DROP TABLE IF EXISTS tempMain_2;
		
		SET @m1=CONCAT(
			'create temporary table tempMain (`Lon` DOUBLE, `Lat` DOUBLE, `App_rate_UL` VARCHAR(20), `App_rate_DL` VARCHAR(20),
				locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select `Lon`, `Lat`, 
					CASE WHEN App_rate_UL <> \'\'
						THEN App_rate_UL
						ELSE NULL
					END 
					AS App_rate_UL,
					CASE WHEN App_rate_DL <> \'\'
						THEN App_rate_DL
						ELSE NULL
					END 
					AS App_rate_DL,
					POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@ueTable,
				' WHERE `App_rate_UL` <>\'\' OR `App_rate_DL` <>\'\''
				);
		
		PREPARE stmt FROM @m1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @m2=CONCAT(
			'create temporary table tempMain_2 (`Lon` DOUBLE, `Lat` DOUBLE, `App_rate_UL` VARCHAR(20), `App_rate_DL` VARCHAR(20),
				locPoint GEOMETRY NOT NULL, INDEX Lon (`Lon`), INDEX Lat (`Lat`), SPATIAL INDEX (locPoint))
				select `Lon`, `Lat`, 
					CASE WHEN App_rate_UL <> \'\'
						THEN App_rate_UL
						ELSE NULL
					END 
					AS App_rate_UL,
					CASE WHEN App_rate_DL <> \'\'
						THEN App_rate_DL
						ELSE NULL
					END 
					AS App_rate_DL, 
					POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \', Lon,\')\')) AS locPoint from ',@ueTable2,
				' WHERE `App_rate_UL` <>\'\' OR `App_rate_DL` <>\'\''
				);
		
		PREPARE stmt FROM @m2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		# find the points inside each polygon bin and take the average values and insert them into final binned table
		
		DROP TABLE IF EXISTS grd_preData;
		DROP TABLE IF EXISTS grd_postData;
		
		SET @d1=CONCAT(
			'CREATE TEMPORARY TABLE grd_preData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, 
					ROUND(AVG(App_rate_UL),3) AS App_rate_UL,
					ROUND(AVG(App_rate_DL),3) AS App_rate_DL
				FROM `tempMain` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons'
				);
		
		PREPARE stmt FROM @d1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @d2=CONCAT(
			'CREATE TEMPORARY TABLE grd_postData (INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				SELECT Y(grd.swPoint) AS Lon, X(grd.swPoint) AS Lat, 
					ROUND(AVG(App_rate_UL),3) AS App_rate_UL,
					ROUND(AVG(App_rate_DL),3) AS App_rate_DL
				FROM `tempMain_2` a, ',@temp_grid,' grd
				WHERE CONTAINS(grd.polygons,a.`locPoint`)
				GROUP BY grd.polygons'
				);
		
		PREPARE stmt FROM @d2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		# find the delta between values of the equally bins from PRE and POST tables
		DROP TABLE IF EXISTS tempDeltaResult;
		
		CREATE TEMPORARY TABLE tempDeltaResult
			SELECT * FROM
				(SELECT a.Lon, a.Lat,
					CASE WHEN ( a.App_rate_UL IS NULL OR b.App_rate_UL IS NULL) 
						THEN NULL
						ELSE ROUND(AVG((b.App_rate_UL-a.App_rate_UL)),3)
					END
					AS App_rate_UL,
					CASE WHEN ( a.App_rate_DL IS NULL OR b.App_rate_DL IS NULL) 
						THEN NULL
						ELSE ROUND(AVG((b.App_rate_DL-a.App_rate_DL)),3) 
					END
					AS App_rate_DL
				FROM grd_preData a 
					JOIN grd_postData b ON a.Lon = b.Lon AND a.Lat = b.Lat 
				GROUP BY Lon, Lat)
			AS d
			WHERE d.App_rate_UL IS NOT NULL OR d.App_rate_DL IS NOT NULL;

		# check if temp delta table is empty - if not, create final ue delta table
		SET @r = (SELECT COUNT(*) FROM tempDeltaResult);
		IF @r <> 0 
			THEN 
			SET @r1=CONCAT('DROP TABLE IF EXISTS ',@ueTableDelta);
		
			PREPARE stmt FROM @r1;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @r2=CONCAT(
				'CREATE TABLE ',@ueTableDelta,' 
					(Lon DOUBLE, Lat DOUBLE, App_rate_UL VARCHAR(20), App_rate_DL VARCHAR(20), 
								INDEX Lon (`Lon`), INDEX Lat (`Lat`))
				');
			PREPARE stmt FROM @r2;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @r3=CONCAT(
				'INSERT INTO ',@ueTableDelta,' (Lon, Lat, App_rate_UL, App_rate_DL)
					SELECT * FROM tempDeltaResult');
								
			PREPARE stmt FROM @r3;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
		END IF;
	END IF;
END WHILE;

SET @grd1 = CONCAT('DROP TABLE IF EXISTS ',@temp_grid);
			
PREPARE stmt FROM @grd1;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

END$$

DELIMITER ;