DELIMITER $$

USE `operator_scanners`$$

DROP PROCEDURE IF EXISTS `createNthBestServer`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `createNthBestServer`(scan_table VARCHAR(100))
BEGIN
#select @neededCellRefDate;
SET @i = CONCAT('SET @neededCellRefDate = (SELECT Needed_Export_Date FROM operator_scanners.master_table WHERE `Name` = \'',scan_table,'\')');
PREPARE stmt0 FROM @i;
EXECUTE stmt0;
DEALLOCATE PREPARE stmt0;

# GSM Best Server
	#Find scanner tables for best server creation
	
DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table, '%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables WHERE table_name LIKE @searchName AND table_name LIKE '%gsm' AND table_name NOT LIKE '%delta%';
	
SET @tables = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @table = (SELECT `names` FROM tableNames WHERE id=@j);
	SET @tables = (CONCAT(@table,',',@tables));
	SET @j = @j+1;
END WHILE;
	# Select scanner table for best server creation
	
WHILE (LOCATE(',', @tables) > 0)
DO
	SET @gsmTable = LEFT(@tables, LOCATE(',',@tables)-1);
	SET @tables = SUBSTRING(@tables, LOCATE(',',@tables) + 1);
	
	# Create temporary 2G_Cell_Ready table with needed CellRefDate from the archive
	
	DROP TABLE IF EXISTS temp_2gNeededCellRef;
	
	CREATE TEMPORARY TABLE temp_2gNeededCellRef
		SELECT * FROM operator.2G_cells_ready_archive
		WHERE export_date = @neededCellRefDate;
	
	# Check if best server table exist
	
	#SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(@gsmTable,'_bestserver'));
	#IF @bestServerExist IS NULL THEN
	
	# Create best server table
	
	SET @d=CONCAT('DROP TABLE IF EXISTS ',@gsmTable,'_bestserver;');
	
	PREPARE stmt FROM @d;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	DROP TABLE IF EXISTS temp_bestserver2g;
	DROP TABLE IF EXISTS temp_top;
	DROP TABLE IF EXISTS temp_second;
	DROP TABLE IF EXISTS temp_third;
	SET @t1=CONCAT(
		'CREATE TEMPORARY TABLE temp_bestserver2g (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
			SELECT *, ROW_NUMBER() OVER (PARTITION BY CONCAT(Lon,Lat) ORDER BY Scanned_level_dBm DESC) AS Ranked 
			FROM (
				SELECT *
				FROM ',@gsmTable,' 
				WHERE (ARFCN >= 00 AND ARFCN <= 00) OR (ARFCN >= 00 AND ARFCN <= 00) OR (ARFCN >= 00 AND ARFCN <= 00)
			) AS submain
			WHERE BSIC IS NOT NULL AND Lon IS NOT NULL AND Lat IS NOT NULL AND Scanned_level_dBm IS NOT NULL'
			);
	PREPARE stmt1 FROM @t1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	CREATE TEMPORARY TABLE temp_top (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
		SELECT * 
			FROM (
				SELECT `Time`, Lon, Lat, Best_Scanned_Level, m.ARFCN, m.BSIC, f.CellName AS Best_CellName, f.CELLID AS Best_CELLID, 
					f.Latitude AS Best_cellLat, f.Longitude AS Best_cellLon, f.Azimuth AS Best_Azimuth, distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS Best_dist_2_cell,
					ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.ARFCN, m.BSIC) ORDER BY Best_dist_2_cell) AS Best_Ranked
				FROM (
					SELECT `Time`, Lon, Lat, Scanned_Level_dBm AS Best_Scanned_Level, ARFCN, BSIC
					FROM temp_bestserver2g
					WHERE Ranked=1
				) AS m
				LEFT OUTER JOIN temp_2gNeededCellRef f ON f.BCCH=m.ARFCN AND f.BSIC_dec=m.BSIC
			) AS best
			WHERE Best_Ranked=1;
				
	CREATE TEMPORARY TABLE temp_second (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
		SELECT * 
			FROM (
				SELECT `Time`, Lon, Lat, SecondBest_Scanned_Level, SecondBest_ARFCN, SecondBest_BSIC, f.CellName AS SecondBest_CellName,  
					f.CELLID AS SecondBest_CELLID, f.Latitude AS SecondBest_cellLat, f.Longitude AS SecondBest_cellLon, 
					f.Azimuth AS SecondBest_Azimuth, distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS SecondBest_dist_2_cell,
					ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, SecondBest_ARFCN, SecondBest_BSIC) ORDER BY SecondBest_dist_2_cell) AS SecondBest_Ranked
				FROM (
					SELECT `Time`, Lon, Lat, Scanned_Level_dBm AS SecondBest_Scanned_Level, ARFCN AS SecondBest_ARFCN, BSIC AS SecondBest_BSIC
					FROM temp_bestserver2g
					WHERE Ranked=2
				) AS m
				LEFT OUTER JOIN temp_2gNeededCellRef f ON f.BCCH=m.SecondBest_ARFCN AND f.BSIC_dec=m.SecondBest_BSIC
			) AS Sec
			WHERE SecondBest_Ranked = 1;
	CREATE TEMPORARY TABLE temp_third (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
		SELECT * 
			FROM (
				SELECT `Time`, Lon, Lat, ThirdBest_Scanned_Level, ThirdBest_ARFCN, ThirdBest_BSIC, f.CellName AS ThirdBest_CellName, 
					 f.CELLID AS ThirdBest_CELLID, f.Latitude AS ThirdBest_cellLat, f.Longitude AS ThirdBest_cellLon, 
					 f.Azimuth AS ThirdBest_Azimuth, distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS ThirdBest_dist_2_cell,
					ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, ThirdBest_ARFCN, ThirdBest_BSIC) ORDER BY ThirdBest_dist_2_cell) AS Thirdbest_Ranked
				FROM (
					SELECT `Time`, Lon, Lat, Scanned_Level_dBm AS ThirdBest_Scanned_Level, ARFCN AS ThirdBest_ARFCN, BSIC AS ThirdBest_BSIC
					FROM temp_bestserver2g
					WHERE Ranked=3
				) AS m
				LEFT OUTER JOIN temp_2gNeededCellRef f ON f.BCCH=m.ThirdBest_ARFCN AND f.BSIC_dec=m.ThirdBest_BSIC
			) AS Third
			WHERE ThirdBest_Ranked = 1;
	
	SET @t2=CONCAT(
		'CREATE TABLE ', @gsmTable, '_bestserver (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
			SELECT 	a.`Time`, a.Lon, a.Lat,
				Best_Scanned_Level, a.ARFCN, a.BSIC, Best_CellName, Best_CELLID, Best_dist_2_cell,
				SecondBest_Scanned_Level, SecondBest_ARFCN, SecondBest_BSIC, SecondBest_CellName, SecondBest_CELLID, SecondBest_dist_2_cell,
				ThirdBest_Scanned_Level, ThirdBest_ARFCN, ThirdBest_BSIC, ThirdBest_CellName, ThirdBest_CELLID, ThirdBest_dist_2_cell
			FROM temp_top a
			LEFT OUTER JOIN temp_second b ON CONCAT(a.Lon,a.Lat) = CONCAT(b.Lon,b.Lat)
			LEFT OUTER JOIN temp_third c ON CONCAT(a.Lon,a.Lat) = CONCAT(c.Lon,c.Lat)'
		);
	PREPARE stmt2 FROM @t2;
	EXECUTE stmt2;
	DEALLOCATE PREPARE stmt2;
END WHILE;
# UMTS Best Servers
	#Find scanner tables for best server creation
	
DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table, '%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables WHERE table_name LIKE @searchName AND table_name LIKE '%umts' AND table_name NOT LIKE '%delta%';
	
SET @tables = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @table = (SELECT `names` FROM tableNames WHERE id=@j);
	SET @tables = (CONCAT(@table,',',@tables));
	SET @j = @j+1;
END WHILE;
	# Select scanner table for best server creation
WHILE (LOCATE(',', @tables) > 0)
DO
	SET @opFreqs3G = '00,00,00,00,00,00,00,';
	SET @umtsTable = LEFT(@tables, LOCATE(',',@tables)-1);
	SET @tables = SUBSTRING(@tables, LOCATE(',',@tables) + 1);
	WHILE (LOCATE(',', @opFreqs3G) > 0)
	DO
		SET @freq3G = LEFT(@opFreqs3G, LOCATE(',',@opFreqs3G)-1);
		SET @opFreqs3G = SUBSTRING(@opFreqs3G, LOCATE(',',@opFreqs3G) + 1);
	
		# Create temporary 3G_Cell_Ready table with needed CellRefDate from the archive
	
		DROP TABLE IF EXISTS temp_3gNeededCellRef;
	
		CREATE TEMPORARY TABLE temp_3gNeededCellRef
			SELECT * FROM operator.3G_cells_ready_archive
			WHERE DL_UARFCN = @freq3G AND export_date = @neededCellRefDate;
	
		# Check if best server table exist
	
		#SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(@umtsTable,'_bestserver_',@freq3G));
		#IF @bestServerExist IS NULL THEN
	
		# Create best server table
		SET @d=CONCAT('DROP TABLE IF EXISTS ',@umtsTable,'_bestserver_',@freq3G,';');
	
		PREPARE stmt FROM @d;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
			DROP TABLE IF EXISTS temp_bestserver3g;
			DROP TABLE IF EXISTS temp_top;
			DROP TABLE IF EXISTS temp_second;
			DROP TABLE IF EXISTS temp_third;
			
			SET @t1=CONCAT(
				'CREATE TEMPORARY TABLE temp_bestserver3g (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`), INDEX `EcIo_dB` (`EcIo_dB`)) 
					SELECT *, ROW_NUMBER() OVER (PARTITION BY CONCAT(Lon,Lat) ORDER BY EcIo_dB DESC) AS Ranked
					FROM ',@umtsTable, 
					' WHERE UARFCN = @freq3G AND Lon IS NOT NULL AND Lat IS NOT NULL AND EcIo_dB IS NOT NULL'
				);
			PREPARE stmt1 FROM @t1;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;		
			IF ((SELECT COUNT(*) FROM temp_bestserver3g)<>0) THEN
				
				CREATE TEMPORARY TABLE temp_top (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.UARFCN, m.PSC, RSCP_dBm, EcIo_dB, Best_EcIo, f.CellName AS Best_CellName, 
							f.CELLID AS Best_CELLID, f.Latitude AS Best_cellLat, f.Longitude AS Best_cellLon, 
							f.Azimuth AS Best_Azimuth, distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS Best_dist_2_cell, 
							g.BCCH AS ARFCN, ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PSC) ORDER BY Best_dist_2_cell) AS Best_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, UARFCN, PSC, RSCP_dBm, EcIo_dB, EcIo_dB AS Best_EcIo 
							FROM temp_bestserver3g
							WHERE Ranked=1
						) AS m
						LEFT OUTER JOIN temp_3gNeededCellRef f ON f.PSC=m.PSC
						LEFT OUTER JOIN operator.2G_cells_ready g ON f.Site = g.Site AND f.Sector_ID = g.Sector_ID
					) AS best
					WHERE Best_Ranked=1;
			
				CREATE TEMPORARY TABLE temp_second (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.UARFCN, m.PSC AS SecondBest_PSC, RSCP_dBm AS SecondBest_RSCP, SecondBest_EcIo, 
							f.CellName AS SecondBest_CellName,  f.CELLID AS SecondBest_CELLID, f.Latitude AS SecondBest_cellLat, 
							f.Longitude AS SecondBest_cellLon, f.Azimuth AS SecondBest_Azimuth, 
							distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS SecondBest_dist_2_cell,
							ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PSC) ORDER BY SecondBest_dist_2_cell) AS SecondBest_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, UARFCN, PSC, RSCP_dBm, EcIo_dB AS SecondBest_EcIo 
							FROM temp_bestserver3g
							WHERE Ranked=2
						) AS m
						LEFT OUTER JOIN temp_3gNeededCellRef f ON f.PSC=m.PSC
					) AS Sec
					WHERE SecondBest_Ranked = 1;
				CREATE TEMPORARY TABLE temp_third (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.UARFCN, m.PSC AS ThirdBest_PSC, RSCP_dBm AS ThirdBest_RSCP, ThirdBest_EcIo, 
							f.CellName AS ThirdBest_CellName, f.CELLID AS ThirdBest_CELLID, f.Latitude AS ThirdBest_cellLat,
							f.Longitude AS ThirdBest_cellLon, f.Azimuth AS ThirdBest_Azimuth, 
							distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS ThirdBest_dist_2_cell,
							ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PSC) ORDER BY ThirdBest_dist_2_cell) AS ThirdBest_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, UARFCN, PSC, RSCP_dBm, EcIo_dB AS ThirdBest_EcIo 
							FROM temp_bestserver3g
							WHERE Ranked=3
						) AS m
						LEFT OUTER JOIN temp_3gNeededCellRef f ON f.PSC=m.PSC
					) AS Third
					WHERE ThirdBest_Ranked=1;
				SET @t2=CONCAT(
					'CREATE TABLE ', @umtsTable, '_bestServer_', @freq3G, ' (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`)) 
						SELECT 	a.`Time`, a.Lon, a.Lat, a.UARFCN, a.PSC, a.RSCP_dBm, a.EcIo_dB, Best_EcIo, Best_CellName, 
							Best_CELLID, Best_dist_2_cell, a.ARFCN,
							SecondBest_PSC, SecondBest_RSCP, SecondBest_EcIo, SecondBest_CellName, SecondBest_CELLID, SecondBest_dist_2_cell,
							ThirdBest_PSC, ThirdBest_RSCP, ThirdBest_EcIo, ThirdBest_CellName, ThirdBest_CELLID, ThirdBest_dist_2_cell
						FROM temp_top a
						LEFT OUTER JOIN temp_second b ON CONCAT(a.Lon,a.Lat) = CONCAT(b.Lon,b.Lat)
						LEFT OUTER JOIN temp_third c ON CONCAT(a.Lon,a.Lat) = CONCAT(c.Lon,c.Lat)'
					);
 
				PREPARE stmt2 FROM @t2;
				EXECUTE stmt2;
				DEALLOCATE PREPARE stmt2;
			END IF;
		#end if;
	END WHILE;
END WHILE;

# LTE Best Servers
	#Find scanner tables for best server creation
	
DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table, '%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables WHERE table_name LIKE @searchName AND table_name LIKE '%lte' AND table_name NOT LIKE '%delta%';
	
SET @tables = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @table = (SELECT `names` FROM tableNames WHERE id=@j);
	SET @tables = (CONCAT(@table,',',@tables));
	SET @j = @j+1;
END WHILE;
	# Select scanner table for best server creation
	
WHILE (LOCATE(',', @tables) > 0)
DO
	SET @opFreqs4G = '00,00,00,00,00,00,';
	SET @lteTable = LEFT(@tables, LOCATE(',',@tables)-1);
	SET @tables = SUBSTRING(@tables, LOCATE(',',@tables) + 1);
	WHILE (LOCATE(',', @opFreqs4G) > 0) 
	DO
		SET @freq4G = LEFT(@opFreqs4G, LOCATE(',',@opFreqs4G)-1);
		SET @opFreqs4G = SUBSTRING(@opFreqs4G, LOCATE(',',@opFreqs4G) + 1);
		# Create temporary 4G_Cell_Ready table with needed CellRefDate from the archive
	
		DROP TABLE IF EXISTS temp_4gNeededCellRef;
	
		CREATE TEMPORARY TABLE temp_4gNeededCellRef
			SELECT * FROM operator.4G_cells_ready_archive
			WHERE DlEarfcn = @freq4G AND export_date = @neededCellRefDate;
	
		# Check if best server table exist
	
		#SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(@lteTable,'_bestserver_',@freq4G));
		#IF @bestServerExist IS NULL THEN
	
			# Create best server table
			
		SET @d=CONCAT('DROP TABLE IF EXISTS ',@lteTable,'_bestserver_',@freq4G,';');
	
		PREPARE stmt FROM @d;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
			DROP TABLE IF EXISTS temp_bestserver4g;
			DROP TABLE IF EXISTS temp_top;
			DROP TABLE IF EXISTS temp_second;
			DROP TABLE IF EXISTS temp_third;
			
			SET @t1=CONCAT(
				'CREATE TEMPORARY TABLE temp_bestserver4g (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`)) 
					SELECT *, ROW_NUMBER() OVER (PARTITION BY CONCAT(Lon,Lat) ORDER BY RSRP DESC) AS Ranked
					FROM ',@lteTable, 
					' WHERE EARFCN = @freq4G'
				);
			PREPARE stmt1 FROM @t1;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;	
			
			IF ((SELECT COUNT(*) FROM temp_bestserver4g)<>0) THEN
				CREATE TEMPORARY TABLE temp_top (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.EARFCN, m.PCI, RSRQ, CINR, RSRP, Best_RSRP, f.CellName AS Best_CellName, 
							f.`Site_LOCELLID` AS Best_Site_LOCELLID, f.Latitude AS Best_cellLat, f.Longitude AS Best_cellLon, 
							f.Azimuth AS Best_Azimuth, distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS Best_dist_2_cell, 
							g.BCCH AS ARFCN, ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PCI) ORDER BY Best_dist_2_cell) AS Best_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, EARFCN, PCI, RSRQ, CINR, RSRP, RSRP AS Best_RSRP
							FROM temp_bestserver4g
							WHERE Ranked=1
						) AS m
						LEFT OUTER JOIN temp_4gNeededCellRef f ON f.PCI=m.PCI
						LEFT OUTER JOIN operator.2G_cells_ready g ON f.Site = g.Site AND f.Sector_ID = g.Sector_ID
					) AS best
					WHERE Best_Ranked=1;
			
				CREATE TEMPORARY TABLE temp_second (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.EARFCN, m.PCI AS SecondBest_PCI, SecondBest_RSRP, RSRQ AS SecondBest_RSRQ, 
							CINR AS SecondBest_CINR, f.CellName AS SecondBest_CellName, 
							f.`Site_LOCELLID` AS SecondBest_Site_LOCELLID, f.Latitude AS SecondBest_cellLat,
							f.Longitude AS SecondBest_cellLon, f.Azimuth AS SecondBest_Azimuth, 
							distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS SecondBest_dist_2_cell,
							ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PCI) ORDER BY SecondBest_dist_2_cell) AS SecondBest_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, EARFCN, PCI, RSRQ, CINR, RSRP AS SecondBest_RSRP 
							FROM temp_bestserver4g
							WHERE Ranked=2
						) AS m
						LEFT OUTER JOIN temp_4gNeededCellRef f ON f.PCI=m.PCI
					) AS Sec
					WHERE SecondBest_Ranked = 1;
				CREATE TEMPORARY TABLE temp_third (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`))
					SELECT * 
					FROM (
						SELECT `Time`, Lon, Lat, m.EARFCN, m.PCI AS ThirdBest_PCI, ThirdBest_RSRP, RSRQ AS ThirdBest_RSRQ, 
							CINR AS ThirdBest_CINR, f.CellName AS ThirdBest_CellName, f.`Site_LOCELLID` AS ThirdBest_Site_LOCELLID, 
							f.Latitude AS ThirdBest_cellLat, f.Longitude AS ThirdBest_cellLon, f.Azimuth AS ThirdBest_Azimuth, 
							distcalc(f.Latitude, f.Longitude, m.Lat, m.Lon) AS ThirdBest_dist_2_cell,
							ROW_NUMBER() OVER (PARTITION BY CONCAT(m.Lon, m.Lat, m.PCI) ORDER BY ThirdBest_dist_2_cell) AS ThirdBest_Ranked
						FROM (
							SELECT `Time`, Lon, Lat, EARFCN, PCI, RSRQ, CINR, RSRP AS ThirdBest_RSRP 
							FROM temp_bestserver4g
							WHERE Ranked=3
						) AS m
						LEFT OUTER JOIN temp_4gNeededCellRef f ON f.PCI=m.PCI
					) AS Third
					WHERE ThirdBest_Ranked=1;
				SET @t2=CONCAT(
					'CREATE TABLE ', @lteTable, '_bestServer_', @freq4G, ' (INDEX `Lon` (`Lon`), INDEX `Lat` (`Lat`)) 
						SELECT a.`Time`, a.Lon, a.Lat,
							a.EARFCN, a.PCI, a.RSRQ, a.CINR, a.RSRP, Best_RSRP, Best_CellName, Best_Site_LOCELLID, Best_dist_2_cell, a.ARFCN,
							SecondBest_PCI, SecondBest_RSRP, SecondBest_RSRQ, SecondBest_CINR, SecondBest_CellName, SecondBest_Site_LOCELLID, SecondBest_dist_2_cell,
							ThirdBest_PCI, ThirdBest_RSRP, ThirdBest_RSRQ, ThirdBest_CINR, ThirdBest_CellName, ThirdBest_Site_LOCELLID, ThirdBest_dist_2_cell
						FROM temp_top a
						LEFT OUTER JOIN temp_second b ON CONCAT(a.Lon,a.Lat) = CONCAT(b.Lon,b.Lat)
						LEFT OUTER JOIN temp_third c ON CONCAT(a.Lon,a.Lat) = CONCAT(c.Lon,c.Lat)'
						);
				PREPARE stmt2 FROM @t2;
				EXECUTE stmt2;
				DEALLOCATE PREPARE stmt2;
			END IF;
		#END IF;
	END WHILE;
END WHILE;
# Create suggested GSM neighbors table
# Check if table of suggested g2g neighbors from scanner exist
	
SET @suggestedNbrsG2G = CONCAT(scan_table,'_suggested_nbrs_g2g');
SET @suggestedNbrsG2GExist = (SELECT table_name FROM information_schema.tables WHERE table_name = @suggestedNbrsG2G);
IF @suggestedNbrsG2GExist IS NULL THEN	
	SET @gsmBestServer = CONCAT(scan_table, '_gsm_bestserver');
	
	DROP TABLE IF EXISTS temp_scang2gnbrs;
	DROP TABLE IF EXISTS temp_2gNeededCellRef;
	
	CREATE TEMPORARY TABLE temp_2gNeededCellRef
		SELECT * FROM operator.2G_cells_ready_archive
		WHERE export_date = @neededCellRefDate;
	
	# Create suggested g2g neighbors table
		
	SET @t1 = CONCAT(
		'CREATE TEMPORARY TABLE temp_scang2gnbrs
			SELECT 	LEFT(Best_CellName,4) AS SRCSite, Best_CellName AS SRCCELLNAME, Best_CELLID AS SRCCELLID, LEFT(SecondBest_CellName,4) AS NBRSite, 
				SecondBest_CellName AS NBRCELLNAME, SecondBest_CELLID AS NBRCELLID, CONCAT(`Best_CellName`,\'_\',`SecondBest_CellName`) AS Suggested_NBRs, 
				distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km, \'Second\' AS Relation_Type, COUNT(*) AS Samples_Count,
				ROUND(AVG(Best_Scanned_Level),1) AS AVG_SRC_RxLevel, 
				ROUND(AVG(SecondBest_Scanned_Level-Best_Scanned_Level),1) AS AVG_Delta, 
				ROUND(MAX(SecondBest_Scanned_Level-Best_Scanned_Level),1) AS MIN_Delta,
				ROUND(STDDEV(SecondBest_Scanned_Level-Best_Scanned_Level),1) AS Deviation
			FROM ',@gsmBestServer, ' g 
			LEFT OUTER JOIN temp_2gNeededCellRef s on g.Best_CellName = s.CELLNAME
			LEFT OUTER JOIN temp_2gNeededCellRef t on g.SecondBest_CellName = t.CELLNAME
			WHERE `Best_CellName` <> `SecondBest_CellName`
			GROUP BY Suggested_NBRs'
			);
			
	PREPARE stmt1 FROM @t1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
	SET @t2 = CONCAT(
		'INSERT INTO temp_scang2gnbrs
			SELECT LEFT(Best_CellName,4) AS SRCSite, Best_CellName AS SRCCELLNAME, Best_CELLID AS SRCCELLID, LEFT(ThirdBest_CellName,4) AS NBRSite, 
				ThirdBest_CellName AS NBRCELLNAME, ThirdBest_CELLID AS NBRCELLID, CONCAT(`Best_CellName`,\'_\',`ThirdBest_CellName`) AS Suggested_NBRs, 
				distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km, \'Third\' AS Relation_Type, COUNT(*) AS Samples_Count,
				ROUND(AVG(Best_Scanned_Level),1) AS AVG_SRC_RxLevel,
				ROUND(AVG(ThirdBest_Scanned_Level-Best_Scanned_Level),1) AS AVG_Delta, 
				ROUND(MAX(ThirdBest_Scanned_Level-Best_Scanned_Level),1) AS MIN_Delta,
				ROUND(STDDEV(ThirdBest_Scanned_Level-Best_Scanned_Level),1) AS Deviation
			FROM ',@gsmBestServer, ' g 
			LEFT OUTER JOIN temp_2gNeededCellRef s on g.Best_CellName = s.CELLNAME
			LEFT OUTER JOIN temp_2gNeededCellRef t on g.ThirdBest_CellName = t.CELLNAME
			WHERE `Best_CellName` <> `ThirdBest_CellName`
			GROUP BY Suggested_NBRs'
			);
	
	PREPARE stmt2 FROM @t2;
	EXECUTE stmt2;
	DEALLOCATE PREPARE stmt2;
	
	DROP TABLE IF EXISTS nbrs_count;
	
	CREATE TEMPORARY TABLE nbrs_count
		SELECT SRCCELLNAME AS CELL, COUNT(*) AS Defined_Nbrs
		FROM operator.nbrs_g2g
		GROUP BY SRCCELLNAME;
		
	SET @t3 = CONCAT(	
		'CREATE TABLE ', @suggestedNbrsG2G,' 
			SELECT 	s.SRCSite, s.SRCCELLNAME, s.SRCCELLID, c.Defined_Nbrs AS SRC_Defined_Nbrs, 
				s.NBRSite, s.NBRCELLNAME, s.NBRCELLID, d.Defined_Nbrs AS NBR_Defined_Nbrs,
				AVG_SRC_RxLevel, AVG_Delta, MIN_Delta, Deviation, Suggested_NBRs, Relation_Type, Samples_Count, Distance_km
			FROM operator_scanners.temp_scang2gnbrs AS s
			LEFT OUTER JOIN operator.`nbrs_g2g` n ON CONCAT(n.SRCCELLNAME,\'_\',n.NBRCELLNAME) = Suggested_NBRs
			LEFT OUTER JOIN nbrs_count c ON s.SRCCELLNAME = c.CELL
			LEFT OUTER JOIN nbrs_count d ON s.NBRCELLNAME = d.CELL
			WHERE CONCAT(n.SRCCELLNAME,\'_\',n.NBRCELLNAME) IS NULL
			ORDER BY Samples_Count DESC'
			);
			
	PREPARE stmt3 FROM @t3;
	EXECUTE stmt3;
	DEALLOCATE PREPARE stmt3;
	
END IF;
# Create suggested UMTS neighbors table
# Set UMTS frequencies for suggested u2u neighbors check
SET @opFreqs3G = '10762,10787,10812,10837,2981,2977,2958,';
# Check if table of suggested u2uintra neighbors from scanner exist
SET @suggestedNbrsU2U = CONCAT(scan_table, '_suggested_nbrs_u2u');
SET @suggestedNbrsU2UExist = (SELECT table_name FROM information_schema.tables WHERE table_name = @suggestedNbrsU2U);
IF @suggestedNbrsU2UExist IS NULL THEN	
	SET @t0 = CONCAT('create table ', @suggestedNbrsU2U, 
			' (SRCSite varchar(10), SRCCELLNAME varchar(10), SRCCELLID varchar(10), SRC_Defined_Nbrs integer, 
			NBRSite varchar(10), NBRCELLNAME varchar(10), NBRCELLID varchar(10), NBR_Defined_Nbrs integer, 
			AVG_SRC_RSCP decimal(5,1), AVG_Delta decimal(5,1), MIN_Delta decimal(5,1), Deviation decimal(5,1),
			Suggested_NBRs varchar(20), Relation_Type varchar(10), Samples_Count integer, Distance_km decimal(10,3))'
			);
			
	PREPARE stmt0 FROM @t0;
	EXECUTE stmt0;
	DEALLOCATE PREPARE stmt0;
	# Create suggested u2u intrafreq neighbors table
	WHILE (LOCATE(',', @opFreqs3G) > 0)
	DO
		SET @freq3G = LEFT(@opFreqs3G, LOCATE(',',@opFreqs3G)-1);
		SET @opFreqs3G = SUBSTRING(@opFreqs3G, LOCATE(',',@opFreqs3G) + 1);
		SET @umtsBestServer = CONCAT(scan_table, '_umts_bestserver_', @freq3G);
		
		IF (SELECT table_name FROM information_schema.tables WHERE table_name = @umtsBestServer) IS NOT NULL THEN
			
			DROP TABLE IF EXISTS temp_scanu2unbrs;
			DROP TABLE IF EXISTS temp_3gNeededCellRef;
	
			CREATE TEMPORARY TABLE temp_3gNeededCellRef
				SELECT * FROM operator.3G_cells_ready_archive
				WHERE export_date = @neededCellRefDate AND DL_UARFCN = @freq3G;
			SET @t1 = CONCAT(
				'CREATE TEMPORARY TABLE temp_scanu2unbrs
					SELECT 	LEFT(Best_CellName,4) AS SRCSite, Best_CellName AS SRCCELLNAME, Best_CELLID AS SRCCELLID, LEFT(SecondBest_CellName,4) AS NBRSite, 
						SecondBest_CellName AS NBRCELLNAME, SecondBest_CELLID AS NBRCELLID, CONCAT(`Best_CellName`,\'_\',`SecondBest_CellName`) AS Suggested_NBRs, 
						distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km, \'Second\' AS Relation_Type, COUNT(*) AS Samples_Count,
						ROUND(AVG(RSCP_dBm),1) AS AVG_SRC_RSCP, 
						ROUND(AVG(SecondBest_RSCP-RSCP_dBm),1) AS AVG_Delta, 
						ROUND(MAX(SecondBest_RSCP-RSCP_dBm),1) AS MIN_Delta,
						ROUND(STDDEV(SecondBest_RSCP-RSCP_dBm),1) AS Deviation
					FROM ',@umtsBestServer, ' u 
					LEFT OUTER JOIN temp_3gNeededCellRef s on u.Best_CellName = s.CELLNAME
					LEFT OUTER JOIN temp_3gNeededCellRef t on u.SecondBest_CellName = t.CELLNAME
					WHERE u.Best_CellName <> u.SecondBest_CellName
					GROUP BY Suggested_NBRs'
					);
				
			PREPARE stmt1 FROM @t1;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
		
			SET @t2 = CONCAT(
				'INSERT INTO temp_scanu2unbrs
					SELECT LEFT(Best_CellName,4) AS SRCSite, Best_CellName AS SRCCELLNAME, Best_CELLID AS SRCCELLID, LEFT(ThirdBest_CellName,4) AS NBRSite, 
						ThirdBest_CellName AS NBRCELLNAME, ThirdBest_CELLID AS NBRCELLID, CONCAT(`Best_CellName`,\'_\',`ThirdBest_CellName`) AS Suggested_NBRs, 
						distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km, \'Third\' AS Relation_Type, COUNT(*) AS Samples_Count,
						ROUND(AVG(RSCP_dBm),1) AS AVG_SRC_RSCP,
						ROUND(AVG(ThirdBest_RSCP-RSCP_dBm),1) AS AVG_Delta, 
						ROUND(MAX(ThirdBest_RSCP-RSCP_dBm),1) AS MIN_Delta,
						ROUND(STDDEV(ThirdBest_RSCP-RSCP_dBm),1) AS Deviation
					FROM ',@umtsBestServer, ' u 
					LEFT OUTER JOIN temp_3gNeededCellRef s on u.Best_CellName = s.CELLNAME
					LEFT OUTER JOIN temp_3gNeededCellRef t on u.ThirdBest_CellName = t.CELLNAME
					WHERE u.Best_CellName <> u.ThirdBest_CellName
					GROUP BY Suggested_NBRs'
					);
		
			PREPARE stmt2 FROM @t2;
			EXECUTE stmt2;
			DEALLOCATE PREPARE stmt2;
			
			DROP TABLE IF EXISTS nbrs_count;
	
			CREATE TEMPORARY TABLE nbrs_count
				SELECT CELLNAME AS CELL, COUNT(*) AS Defined_Nbrs
				FROM operator.nbrs_u2uintra
				GROUP BY CELLNAME;
			SET @t3 = CONCAT(	
				'INSERT INTO ', @suggestedNbrsU2U,' 
					SELECT 	s.SRCSite, s.SRCCELLNAME, s.SRCCELLID, c.Defined_Nbrs AS SRC_Defined_Nbrs, 
						s.NBRSite, s.NBRCELLNAME, s.NBRCELLID, d.Defined_Nbrs AS NBR_Defined_Nbrs,
						AVG_SRC_RSCP, AVG_Delta, MIN_Delta, Deviation, 
						Suggested_NBRs, Relation_Type, Samples_Count, Distance_km
					FROM temp_scanu2unbrs s
					LEFT OUTER JOIN operator.`nbrs_u2uintra` n ON CONCAT(n.CELLNAME,\'_\',n.NCELLNAME) = Suggested_NBRs
					LEFT OUTER JOIN nbrs_count c ON s.SRCCELLNAME = c.CELL
					LEFT OUTER JOIN nbrs_count d ON s.NBRCELLNAME = d.CELL
					WHERE CONCAT(n.CELLNAME,\'_\',n.NCELLNAME) IS NULL
					ORDER BY Samples_Count DESC'
					);
					
			PREPARE stmt3 FROM @t3;
			EXECUTE stmt3;
			DEALLOCATE PREPARE stmt3;
		END IF;
		
	END WHILE;
END IF;

# Create suggested U2G neighbors table
# Set UMTS frequencies for suggested u2u neighbors check
SET @gsmBestServer = CONCAT(scan_table, '_gsm_bestserver');
SET @opFreqs3G = '10762,10787,10812,10837,2981,2977,2958,';
# Check if table of suggested u2uintra neighbors from scanner exist
SET @suggestedNbrsU2G = CONCAT(scan_table, '_suggested_nbrs_u2g');
SET @suggestedNbrsU2GExist = (SELECT table_name FROM information_schema.tables WHERE table_name = @suggestedNbrsU2G);
IF @suggestedNbrsU2GExist IS NULL THEN	
	SET @t0 = CONCAT('create table ', @suggestedNbrsU2G, 
			' (SRCSite varchar(10), SRCCELLNAME varchar(10), SRCCELLID varchar(10), SRC_Defined_Nbrs integer, 
			NBRSite varchar(10), NBRCELLNAME varchar(10), NBRCELLID varchar(10), 
			AVG_SRC_RSCP decimal(5,1), AVG_Delta decimal(5,1), MIN_Delta decimal(5,1), Deviation decimal(5,1),
			Suggested_NBRs varchar(20), Suggested_CI_NBRs varchar(20), Relation_Type varchar(10), Samples_Count integer, Distance_km decimal(10,3))'
			);
			
	PREPARE stmt0 FROM @t0;
	EXECUTE stmt0;
	DEALLOCATE PREPARE stmt0;
	
	DROP TABLE IF EXISTS nbrs_count;
	
	CREATE TEMPORARY TABLE nbrs_count
		SELECT CELLNAME AS CELL, COUNT(*) AS Defined_Nbrs
		FROM operator.nbrs_u2g
		GROUP BY CELLNAME;	
	
	# Create suggested u2g intrafreq neighbors table
	WHILE (LOCATE(',', @opFreqs3G) > 0)
	DO
		SET @freq3G = LEFT(@opFreqs3G, LOCATE(',',@opFreqs3G)-1);
		SET @opFreqs3G = SUBSTRING(@opFreqs3G, LOCATE(',',@opFreqs3G) + 1);
		SET @umtsBestServer = CONCAT(scan_table, '_umts_bestserver_', @freq3G);
		
		IF (SELECT table_name FROM information_schema.tables WHERE table_name = @umtsBestServer) IS NOT NULL THEN
			
			DROP TABLE IF EXISTS temp_3gNeededCellRef;
	
			CREATE TEMPORARY TABLE temp_3gNeededCellRef
				SELECT * FROM operator.3G_cells_ready_archive
				WHERE export_date = @neededCellRefDate AND DL_UARFCN = @freq3G;
			# Create suggested u2g neighbors table	
		
			SET @t1 = CONCAT(
				'INSERT INTO ', @suggestedNbrsU2G, ' 
					SELECT 	*
					FROM (
						SELECT	LEFT(u.Best_CellName,4) AS SRCSite, u.Best_CellName AS SRCCELLNAME, 
							u.Best_CELLID AS SRCCELLID, n.Defined_Nbrs AS SRC_Defined_Nbrs, 
							LEFT(g.Best_CellName,4) AS NBRSite, 
							g.Best_CellName AS NBRCELLNAME, g.Best_CELLID AS NBRCELLID, 
							ROUND(AVG(u.RSCP_dBm),1) AS AVG_SRC_RSCP, 
							ROUND(AVG(g.Best_Scanned_Level-u.RSCP_dBm),1) AS AVG_Delta, 
							ROUND(MAX(g.Best_Scanned_Level-u.RSCP_dBm),1) AS MIN_Delta,
							ROUND(STDDEV(g.Best_Scanned_Level-u.RSCP_dBm),1) AS Deviation,
							CONCAT(u.`Best_CellName`,\'_\',g.`Best_CellName`) AS Suggested_NBRs, 
							CONCAT(u.`Best_CellID`,\'_\',g.`Best_CellID`) AS Suggested_CI_NBRs,
							\'Top\' AS Relation_Type, COUNT(*) AS Samples_Count,
							distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km
						FROM ',@umtsBestServer, ' u
						JOIN ', @gsmBestServer, ' g on g.Lon = u.Lon AND g.Lat = u.Lat
						LEFT OUTER JOIN nbrs_count n on n.CELL = u.Best_CellName
						LEFT OUTER JOIN operator.nbrs_u2g i on i.CELLNAME = u.Best_CellName AND i.GSMCELLINDEX = g.Best_CELLID
						LEFT OUTER JOIN temp_3gNeededCellRef s on u.Best_CellName = s.CELLNAME
						LEFT OUTER JOIN temp_2gNeededCellRef t on g.Best_CellName = t.CELLNAME
						WHERE i.GSMCELLINDEX IS NULL
						GROUP BY Suggested_NBRs
					) as a
					WHERE Distance_km <= 25
					ORDER BY Samples_Count DESC'
				);
			
			PREPARE stmt1 FROM @t1;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
		END IF;
		
	END WHILE;
END IF;

/*
# Create suggested l2u neighbors table
# Set LTE and UMTS frequencies for suggested l2u neighbors check
SET @opFreqs4G = '00,00,00,00,00,00,';
SET @opFreqs3G = '00,00,00,00,00,00,00,';
# Check if table of suggested l2u neighbors from scanner exist
SET @suggestedNbrsL2U = CONCAT(scan_table, '_suggested_nbrs_l2u');
SET @suggestedNbrsL2UExist = (SELECT table_name FROM information_schema.tables WHERE table_name = @suggestedNbrsL2U);
IF @suggestedNbrsL2UExist IS NULL THEN	
	SET @t0 = CONCAT('create table ', @suggestedNbrsL2U, 
			' (SRCSite varchar(10), SRCCELLNAME varchar(10), SRCCELLID varchar(10), SRC_Defined_Nbrs integer, 
			NBRSite varchar(10), NBRCELLNAME varchar(10), NBRCELLID varchar(10), 
			AVG_SRC_RSCP decimal(5,1), AVG_Delta decimal(5,1), MIN_Delta decimal(5,1), Deviation decimal(5,1),
			Suggested_NBRs varchar(20), Suggested_CI_NBRs varchar(20), Relation_Type varchar(10), Samples_Count integer, Distance_km decimal(10,3))'
			);
			
	PREPARE stmt0 FROM @t0;
	EXECUTE stmt0;
	DEALLOCATE PREPARE stmt0;
	
	DROP TABLE IF EXISTS nbrs_count;
	
	CREATE TEMPORARY TABLE nbrs_count
		SELECT LocalCellName AS CELL, COUNT(*) AS Defined_Nbrs
		FROM operator.nbrs_l2u
		GROUP BY LocalCellName;	
	
	# Create suggested l2u neighbors table
	WHILE (LOCATE(',', @opFreqs4G) > 0) 
	DO
		SET @freq4G = LEFT(@opFreqs4G, LOCATE(',',@opFreqs4G)-1);
		SET @opFreqs4G = SUBSTRING(@opFreqs4G, LOCATE(',',@opFreqs4G) + 1);
		SET @lteBestServer = CONCAT(scan_table, '_lte_bestserver_', @freq4G);
		
		IF (SELECT table_name FROM information_schema.tables WHERE table_name = @lteBestServer) IS NOT NULL THEN
		
			# Create temporary 4G_Cell_Ready table with needed CellRefDate from the archive
	
			DROP TABLE IF EXISTS temp_4gNeededCellRef;
	
			CREATE TEMPORARY TABLE temp_4gNeededCellRef
				SELECT * FROM operator.4G_cells_ready_archive
				WHERE DlEarfcn = @freq4G AND export_date = @neededCellRefDate;
	
			WHILE (LOCATE(',', @opFreqs3G) > 0)
			DO
				SET @freq3G = LEFT(@opFreqs3G, LOCATE(',',@opFreqs3G)-1);
				SET @opFreqs3G = SUBSTRING(@opFreqs3G, LOCATE(',',@opFreqs3G) + 1);
				SET @umtsBestServer = CONCAT(scan_table, '_umts_bestserver_', @freq3G);
			
				IF (SELECT table_name FROM information_schema.tables WHERE table_name = @umtsBestServer) IS NOT NULL THEN
				
					DROP TABLE IF EXISTS temp_3gNeededCellRef;
		
					CREATE TEMPORARY TABLE temp_3gNeededCellRef
						SELECT * FROM operator.3G_cells_ready_archive
						WHERE export_date = @neededCellRefDate AND DL_UARFCN = @freq3G;
					# Create suggested l2u neighbors table	
				
					SET @t1 = CONCAT(
						'INSERT INTO ', @suggestedNbrsL2U, ' 
							SELECT 	*
							FROM (
								SELECT	LEFT(l.Best_CellName,4) AS SRCSite, l.Best_CellName AS SRCCELLNAME, 
									l.Best_Site_LOCELLID AS SRCCELLID, n.Defined_Nbrs AS SRC_Defined_Nbrs, 
									LEFT(u.Best_CellName,4) AS NBRSite, 
									u.Best_CellName AS NBRCELLNAME, u.Best_CELLID AS NBRCELLID, 
									ROUND(AVG(l.Best_RSRP),1) AS AVG_SRC_RSRP, 
									ROUND(AVG(u.RSCP_dBm-l.Best_RSRP),1) AS AVG_Delta, 
									ROUND(MAX(u.RSCP_dBm-l.Best_RSRP),1) AS MIN_Delta,
									ROUND(STDDEV(u.RSCP_dBm-l.Best_RSRP),1) AS Deviation,
									CONCAT(l.`Best_CellName`,\'_\',u.`Best_CellName`) AS Suggested_NBRs, 
									CONCAT(l.`Best_Site_LOCELLID`,\'_\',u.`Best_CellID`) AS Suggested_CI_NBRs,
									\'Top\' AS Relation_Type, COUNT(*) AS Samples_Count,
									distcalc(s.Latitude, s.Longitude, t.Latitude, t.Longitude) AS Distance_km
								FROM ',@lteBestServer, ' l
								JOIN ', @umtsBestServer, ' u on u.Lon = l.Lon AND u.Lat = l.Lat
								LEFT OUTER JOIN nbrs_count n on n.CELL = l.Best_CellName
								LEFT OUTER JOIN operator.nbrs_l2u i on i.LocalCellName = l.Best_CellName AND i.NeighbourCellName = u.Best_CELLNAME
								LEFT OUTER JOIN temp_4gNeededCellRef s on l.Best_CellName = s.CELLNAME
								LEFT OUTER JOIN temp_3gNeededCellRef t on u.Best_CellName = t.CELLNAME
								WHERE i.NeighbourCellName IS NULL
								GROUP BY Suggested_NBRs
							) as a
							WHERE Distance_km <= 25
							ORDER BY Samples_Count DESC'
						);
					
					PREPARE stmt1 FROM @t1;
					EXECUTE stmt1;
					DEALLOCATE PREPARE stmt1;
		
				END IF;
		
			END WHILE;
		END IF;
	END WHILE;
END IF;
*/

END$$

DELIMITER ;