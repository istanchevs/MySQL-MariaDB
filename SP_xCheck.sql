DELIMITER $$

USE `operator_scanners`$$

DROP PROCEDURE IF EXISTS `xCheck`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `xCheck`(scan_table VARCHAR(100))

BEGIN

SET @xCheckTable = CONCAT(scan_table,'_xcheck');

#find needed CellRef date

SET @i = CONCAT('SET @neededCellRefDate = (SELECT Needed_Export_Date FROM operator_scanners.master_table WHERE `Name` = \'',scan_table,'\')');
PREPARE stmt FROM @i;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

# 2G cross check

SET @j=	CONCAT('SET @g = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',scan_table,'_gsm_bestserver\')');

PREPARE stmtG FROM @j;
EXECUTE stmtG;
DEALLOCATE PREPARE stmtG;

IF @g IS NOT NULL 
	THEN
	
	SET @best2gTable = CONCAT(scan_table,'_gsm_bestserver');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@xCheckTable);
	
	PREPARE stmt1 FROM @t1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
			SET @t2=CONCAT(
				'CREATE TABLE ',@xCheckTable,' (Site VARCHAR(5), CellName VARCHAR(10), Channel VARCHAR(10), Azimuth VARCHAR(5), HBeam VARCHAR(5), LenghtSector DECIMAL(6,3), 
								BeamSearch DECIMAL(4,1), DistSearch DECIMAL(8,3), 
								Min_dist2cell DECIMAL(8,3), Mean_dist2cell DECIMAL(8,3), 
								Min_RxLev DECIMAL(6,1), Mean_RxLev DECIMAL(6,1),
								BestCellWithinBeamSearch_count INTEGER, BestCellAllSamples_count INTEGER, CrossProbability_percent DECIMAL(4,1))'
				);
		
	PREPARE stmt FROM @t2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	DROP TABLE IF EXISTS temp_xcheck;
	DROP TABLE IF EXISTS temp_cellsForXcheck;

	SET @t3 = CONCAT('CREATE TEMPORARY TABLE temp_xcheck (locPoint GEOMETRY NOT NULL, 
				SPATIAL INDEX `locPoint` (`locPoint`))
		SELECT *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \',Lon,\')\')) AS locPoint FROM ',@best2gTable
		);
		
	PREPARE stmt FROM @t3;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @t4 = CONCAT('CREATE TEMPORARY TABLE temp_cellsForXcheck (PolyCellHBeam GEOMETRY NOT NULL, SPATIAL INDEX PolyCellHBeam (PolyCellHBeam))
						SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
							Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
							AzimuthMinusHBeam, Azimuth,AzimuthPlusHBeam, HBeam, DistSearch, LenghtSector,
							POLYGONFROMTEXT(CONCAT(\'POLYGON((\',X(locA),\' \', Y(locA),\',\',X(locB),\' \',Y(locB),\',\',X(locC),\' \',Y(locC),\',\',X(locD),\' \',Y(locD),\',\',X(locE),\' \',Y(locE),\',\',X(locF),\' \',Y(locF),\',\',X(locA),\' \',Y(locA),\'))\')) AS PolyCellHBeam
						FROM (
							SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
								Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
								AzimuthMinusHBeam, AzimuthMinusHalfHBeam, Azimuth, AzimuthPlusHalfHBeam, AzimuthPlusHBeam, HBeam, 
								Round(DistLength/1000,2) as DistSearch, LenghtSector, 
								locA, 
								dLocationGeom(locA, AzimuthMinusHBeam, DistLength) AS locB,
								dLocationGeom(locA, AzimuthMinusHalfHBeam, DistLength) AS locC,
								dLocationGeom(locA, Azimuth, DistLength) AS locD,
								dLocationGeom(locA, AzimuthPlusHalfHBeam, DistLength) AS locE,
								dLocationGeom(locA, AzimuthPlusHBeam, DistLength) AS locF
							FROM ( 
								SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
									Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
									POINTFROMTEXT(CONCAT(\'POINT(\',latitude,\' \',longitude,\')\')) AS locA,
									CASE WHEN Azimuth-HBeam <0 
										THEN Round(360+(Azimuth-HBeam),1)
										ELSE Round(Azimuth-HBeam,1)
									END AS AzimuthMinusHBeam, 
									CASE WHEN Azimuth-HBeam/2 <0 
										THEN Round(360+(Azimuth-HBeam/2),1)
										ELSE Round(Azimuth-HBeam/2,1)
									END AS AzimuthMinusHalfHBeam, 
									Azimuth, 
									CASE WHEN Azimuth+HBeam/2>360
										THEN round(Azimuth+HBeam/2-360,1)
										ELSE round(Azimuth+HBeam/2,1)
									END AS AzimuthPlusHalfHBeam, 
									CASE WHEN Azimuth+HBeam>360
										THEN round(Azimuth+HBeam-360,1)
										ELSE round(Azimuth+HBeam,1)
									END AS AzimuthPlusHBeam, 
									HBeam,
									CASE WHEN Max_dist_2_cell*1.1 > 35 
										THEN 35000
										ELSE Max_dist_2_cell*1.1*1000
									END 
									AS DistLength,
									LenghtSector
								FROM (
									SELECT 	Site, CELLNAME, \'GSM\' AS Channel, longitude, latitude, 
										ROUND(MIN(`Best_dist_2_cell`),3) as Min_dist_2_cell, 
										ROUND(AVG(`Best_dist_2_cell`),3) as Mean_dist_2_cell,
										ROUND(MAX(`Best_dist_2_cell`),3) as Max_dist_2_cell,
										ROUND(MIN(`Best_Scanned_Level`),1) as Min_RxLev, 
										ROUND(AVG(`Best_Scanned_Level`),1) as Mean_RxLev,
										(CASE WHEN Azimuth REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN Azimuth ELSE 1 END) AS Azimuth,
										(CASE WHEN HBeam REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(HBeam,1) ELSE 60.0 END) AS HBeam,
										(CASE WHEN LenghtSector REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(LenghtSector,3) ELSE 1 END) AS LenghtSector											
									FROM ',@best2gTable, ' d
									LEFT OUTER JOIN operator.2g_cells_ready_archive c ON d.best_cellname = c.CELLNAME
									WHERE export_date = \'',@neededCellRefDate,'\' AND antenna NOT LIKE \'%738445%\' AND antenna NOT LIKE \'%738446%\' AND HBeam <> 360
									GROUP BY Cellname
									) as d
								) AS a
							) AS b'
					);

	PREPARE stmt FROM @t4;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @t5 = CONCAT('INSERT INTO ',@xCheckTable,' 
					SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, 2*HBeam as BeamSearch, DistSearch, 
						Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
						BestCellWithinBeamSearch_count, BestCellAllSamples_count, round(100*(1-BestCellWithinBeamSearch_count/BestCellAllSamples_count),1) AS CrossProbability_percent
					FROM (
						SELECT  c.Site, c.CELLNAME, c.Channel, c.Azimuth, c.HBeam, c.LenghtSector, c.DistSearch, 
						c.Min_dist_2_cell, c.Mean_dist_2_cell, c.Min_RxLev, c.Mean_RxLev,
							CASE WHEN e.BestCellWithinBeamSearch_count IS NOT NULL 
								THEN e.BestCellWithinBeamSearch_count
								ELSE 0 
							END AS BestCellWithinBeamSearch_count
						FROM (
							SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, COUNT(locPoint) AS BestCellWithinBeamSearch_count								
							FROM (
								SELECT Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, locPoint
								FROM `temp_cellsforxcheck` c
								LEFT JOIN `temp_xcheck` t ON c.CELLNAME = t.Best_CellName
								WHERE ST_CONTAINS(PolyCellHBeam,locPoint)
								) AS d
								GROUP BY CELLNAME
								) as e
						RIGHT JOIN `temp_cellsforxcheck` c on e.CELLNAME = c.CELLNAME
						) as a
					LEFT JOIN (
							SELECT `Best_CellName`, COUNT(*) AS BestCellAllSamples_count
							FROM `temp_xcheck`
							GROUP BY `Best_CellName`
						) AS b ON a.CELLNAME = b.Best_CellName
					WHERE Min_dist_2_cell <= 15 AND BestCellAllSamples_count >4
					ORDER BY CrossProbability_percent desc'
				);
			
	PREPARE stmt FROM @t5;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END IF;

# 3G cross check

# if at least one best server table exist then check for crosses per frequency

SET @u = (SELECT table_name FROM information_schema.tables WHERE table_name LIKE CONCAT(scan_table,'_umts_bestserver%') LIMIT 1);
IF @u IS NOT NULL
	THEN
	
	SET @opFreqs3G = '00,00,00,00,00,00,00,';
		
	# check if xcheck table is already created 

	IF @g IS NULL 
		THEN

		# create xcheck table
		
		SET @t1=CONCAT('DROP TABLE IF EXISTS ',@xCheckTable);
		
		PREPARE stmt FROM @t1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @t2=CONCAT(
				'CREATE TABLE ',@xCheckTable,' (Site VARCHAR(5), CellName VARCHAR(10), Channel VARCHAR(10), Azimuth VARCHAR(5), HBeam VARCHAR(5), LenghtSector DECIMAL(6,3), 
								BeamSearch DECIMAL(4,1), DistSearch DECIMAL(8,3), 
								Min_dist2cell DECIMAL(8,3), Mean_dist2cell DECIMAL(8,3), 
								Min_RxLev DECIMAL(6,1), Mean_RxLev DECIMAL(6,1),
								BestCellWithinBeamSearch_count INTEGER, BestCellAllSamples_count INTEGER, CrossProbability_percent DECIMAL(4,1))'
				);
			
		PREPARE stmt FROM @t2;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
	END IF;
	
	# select 3g frequency to check corresponding best server table for crosses
	
	WHILE (LOCATE(',', @opFreqs3G) > 0)
		DO
		SET @freq3G = LEFT(@opFreqs3G, LOCATE(',',@opFreqs3G)-1);
		SET @opFreqs3G = SUBSTRING(@opFreqs3G, LOCATE(',',@opFreqs3G) + 1);
	
		# check if best server table for selected frequency exist 
	
		SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(scan_table,'_umts_bestserver_',@freq3G));
		IF @bestServerExist IS NOT NULL 
			THEN
			
			DROP TABLE IF EXISTS temp_xcheck;
			DROP TABLE IF EXISTS temp_cellsForXcheck;
			
			SET @best3gTable = CONCAT(scan_table,'_umts_bestserver_',@freq3G);
			IF @freq3G IN ('00','00','00')
				THEN SET @min_dist = 15;
				ELSE SET @min_dist = 10;
			END IF;
			
			SET @t3 = CONCAT('CREATE TEMPORARY TABLE temp_xcheck (locPoint GEOMETRY NOT NULL, 
						SPATIAL INDEX `locPoint` (`locPoint`))
				SELECT *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \',Lon,\')\')) AS locPoint FROM ',@best3gTable
				);
				
			PREPARE stmt FROM @t3;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
	SET @t4 = CONCAT('CREATE TEMPORARY TABLE temp_cellsForXcheck (PolyCellHBeam GEOMETRY NOT NULL, SPATIAL INDEX PolyCellHBeam (PolyCellHBeam))
						SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
							Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
							AzimuthMinusHBeam, Azimuth,AzimuthPlusHBeam, HBeam, DistSearch, LenghtSector,
							POLYGONFROMTEXT(CONCAT(\'POLYGON((\',X(locA),\' \', Y(locA),\',\',X(locB),\' \',Y(locB),\',\',X(locC),\' \',Y(locC),\',\',X(locD),\' \',Y(locD),\',\',X(locE),\' \',Y(locE),\',\',X(locF),\' \',Y(locF),\',\',X(locA),\' \',Y(locA),\'))\')) AS PolyCellHBeam
						FROM (
							SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
								Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
								AzimuthMinusHBeam, AzimuthMinusHalfHBeam, Azimuth, AzimuthPlusHalfHBeam, AzimuthPlusHBeam, HBeam, 
								Round(DistLength/1000,2) as DistSearch, LenghtSector, 
								locA, 
								dLocationGeom(locA, AzimuthMinusHBeam, DistLength) AS locB,
								dLocationGeom(locA, AzimuthMinusHalfHBeam, DistLength) AS locC,
								dLocationGeom(locA, Azimuth, DistLength) AS locD,
								dLocationGeom(locA, AzimuthPlusHalfHBeam, DistLength) AS locE,
								dLocationGeom(locA, AzimuthPlusHBeam, DistLength) AS locF
							FROM ( 
								SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
									Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
									POINTFROMTEXT(CONCAT(\'POINT(\',latitude,\' \',longitude,\')\')) AS locA,
									CASE WHEN Azimuth-HBeam <0 
										THEN Round(360+(Azimuth-HBeam),1)
										ELSE Round(Azimuth-HBeam,1)
									END AS AzimuthMinusHBeam, 
									CASE WHEN Azimuth-HBeam/2 <0 
										THEN Round(360+(Azimuth-HBeam/2),1)
										ELSE Round(Azimuth-HBeam/2,1)
									END AS AzimuthMinusHalfHBeam, 
									Azimuth, 
									CASE WHEN Azimuth+HBeam/2>360
										THEN round(Azimuth+HBeam/2-360,1)
										ELSE round(Azimuth+HBeam/2,1)
									END AS AzimuthPlusHalfHBeam, 
									CASE WHEN Azimuth+HBeam>360
										THEN round(Azimuth+HBeam-360,1)
										ELSE round(Azimuth+HBeam,1)
									END AS AzimuthPlusHBeam, 
									HBeam,
									CASE WHEN Max_dist_2_cell*1.1 > 35 
										THEN 35000
										ELSE Max_dist_2_cell*1.1*1000
									END 
									AS DistLength,
									LenghtSector
								FROM (
									SELECT 	Site, CELLNAME, c.DL_UARFCN AS Channel,  longitude, latitude, 
										ROUND(MIN(`Best_dist_2_cell`),3) as Min_dist_2_cell, 
										ROUND(AVG(`Best_dist_2_cell`),3) as Mean_dist_2_cell,
										ROUND(MAX(`Best_dist_2_cell`),3) as Max_dist_2_cell,
										ROUND(MIN(`RSCP_dBm`),1) as Min_RxLev, 
										ROUND(AVG(`RSCP_dBm`),1) as Mean_RxLev,
										(CASE WHEN Azimuth REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN Azimuth ELSE 1 END) AS Azimuth,
										(CASE WHEN HBeam REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(HBeam,1) ELSE 60.0 END) AS HBeam,
										(CASE WHEN LenghtSector REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(LenghtSector,3) ELSE 1 END) AS LenghtSector										
									FROM ',@best3gTable, ' d
									LEFT OUTER JOIN operator.3g_cells_ready_archive c ON d.best_cellname = c.CELLNAME
									WHERE export_date = \'',@neededCellRefDate,'\' AND DL_UARFCN = ',@freq3G,' 
									AND antenna NOT LIKE \'%738445%\' AND antenna NOT LIKE \'%738446%\' AND HBeam <> 360
									GROUP BY Cellname
									) as d
								) AS a
							) AS b'
					);

			PREPARE stmt FROM @t4;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @t5 = CONCAT('INSERT INTO ',@xCheckTable,' 
					SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, 2*HBeam as BeamSearch, DistSearch, 
						Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
						BestCellWithinBeamSearch_count, BestCellAllSamples_count, round(100*(1-BestCellWithinBeamSearch_count/BestCellAllSamples_count),1) AS CrossProbability_percent
						FROM (
							SELECT  c.Site, c.CELLNAME, c.Channel, c.Azimuth, c.HBeam, c.LenghtSector, c.DistSearch, 
							c.Min_dist_2_cell, c.Mean_dist_2_cell, c.Min_RxLev, c.Mean_RxLev,
								CASE WHEN e.BestCellWithinBeamSearch_count IS NOT NULL 
									THEN e.BestCellWithinBeamSearch_count
									ELSE 0 
								END AS BestCellWithinBeamSearch_count
							FROM (
								SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, COUNT(locPoint) AS BestCellWithinBeamSearch_count								
								FROM (
									SELECT Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, locPoint
									FROM `temp_cellsforxcheck` c
									LEFT JOIN `temp_xcheck` t ON c.CELLNAME = t.Best_CellName
									WHERE ST_CONTAINS(PolyCellHBeam,locPoint)
									) AS d
								GROUP BY CELLNAME
								) as e
							RIGHT JOIN `temp_cellsforxcheck` c on e.CELLNAME = c.CELLNAME
							) as a
						LEFT JOIN (
								SELECT `Best_CellName`, COUNT(*) AS BestCellAllSamples_count
								FROM `temp_xcheck`
								GROUP BY `Best_CellName`
							) AS b ON a.CELLNAME = b.Best_CellName
						WHERE Min_dist_2_cell <= ',@min_dist,' AND BestCellAllSamples_count >4
						ORDER BY CrossProbability_percent desc'
					);
					
			PREPARE stmt FROM @t5;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
		END IF;
	
	END WHILE;
END IF;

# 4G cross check

# if at least one best server table exist then check for crosses per frequency

SET @l = (SELECT table_name FROM information_schema.tables WHERE table_name LIKE CONCAT(scan_table,'_lte_bestserver%') LIMIT 1);
IF @l IS NOT NULL
	THEN
	
	SET @opFreqs4G = '00,00,00,00,00,00,';
	
		# check if xcheck table is already created 

	IF @g IS NULL 
		THEN
		IF @u IS NULL
			THEN

			# create xcheck table
			
			SET @t1=CONCAT('DROP TABLE IF EXISTS ',@xCheckTable);
			
			PREPARE stmt FROM @t1;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @t2=CONCAT(
				'CREATE TABLE ',@xCheckTable,' (Site VARCHAR(5), CellName VARCHAR(10), Channel VARCHAR(10), Azimuth VARCHAR(5), HBeam VARCHAR(5), LenghtSector DECIMAL(6,3), 
								BeamSearch DECIMAL(4,1), DistSearch DECIMAL(8,3), 
								Min_dist2cell DECIMAL(8,3), Mean_dist2cell DECIMAL(8,3), 
								Min_RxLev DECIMAL(6,1), Mean_RxLev DECIMAL(6,1),
								BestCellWithinBeamSearch_count INTEGER, BestCellAllSamples_count INTEGER, CrossProbability_percent DECIMAL(4,1))'
				);
				
			PREPARE stmt FROM @t2;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
		END IF;
	END IF;
	
	WHILE (LOCATE(',', @opFreqs4G) > 0)
		DO
		SET @freq4G = LEFT(@opFreqs4G, LOCATE(',',@opFreqs4G)-1);
		SET @opFreqs4G = SUBSTRING(@opFreqs4G, LOCATE(',',@opFreqs4G) + 1);
	
		# check if best server table for selected frequency exist 
	
		SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(scan_table,'_lte_bestserver_',@freq4G));
		IF @bestServerExist IS NOT NULL 
			THEN
			
			DROP TABLE IF EXISTS temp_xcheck;
			DROP TABLE IF EXISTS temp_cellsForXcheck;
			
			SET @best4gTable = CONCAT(scan_table,'_lte_bestserver_',@freq4G);
			IF @freq4G = '00' 
				THEN SET @min_dist = 15;
				ELSE SET @min_dist = 10;
			END IF;
			
			SET @t3 = CONCAT('CREATE TEMPORARY TABLE temp_xcheck (locPoint GEOMETRY NOT NULL, 
						SPATIAL INDEX `locPoint` (`locPoint`))
				SELECT *, POINTFROMTEXT(CONCAT(\'POINT(\',Lat,\' \',Lon,\')\')) AS locPoint FROM ',@best4gTable
				);
				
			PREPARE stmt FROM @t3;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
	SET @t4 = CONCAT('CREATE TEMPORARY TABLE temp_cellsForXcheck (PolyCellHBeam GEOMETRY NOT NULL, SPATIAL INDEX PolyCellHBeam (PolyCellHBeam))
						SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
							Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
							AzimuthMinusHBeam, Azimuth,AzimuthPlusHBeam, HBeam, DistSearch, LenghtSector,
							POLYGONFROMTEXT(CONCAT(\'POLYGON((\',X(locA),\' \', Y(locA),\',\',X(locB),\' \',Y(locB),\',\',X(locC),\' \',Y(locC),\',\',X(locD),\' \',Y(locD),\',\',X(locE),\' \',Y(locE),\',\',X(locF),\' \',Y(locF),\',\',X(locA),\' \',Y(locA),\'))\')) AS PolyCellHBeam
						FROM (
							SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
								Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
								AzimuthMinusHBeam, AzimuthMinusHalfHBeam, Azimuth, AzimuthPlusHalfHBeam, AzimuthPlusHBeam, HBeam, 
								Round(DistLength/1000,2) as DistSearch, LenghtSector, 
								locA, 
								dLocationGeom(locA, AzimuthMinusHBeam, DistLength) AS locB,
								dLocationGeom(locA, AzimuthMinusHalfHBeam, DistLength) AS locC,
								dLocationGeom(locA, Azimuth, DistLength) AS locD,
								dLocationGeom(locA, AzimuthPlusHalfHBeam, DistLength) AS locE,
								dLocationGeom(locA, AzimuthPlusHBeam, DistLength) AS locF
							FROM ( 
								SELECT 	Site, CELLNAME, Channel, longitude, latitude, 
									Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
									POINTFROMTEXT(CONCAT(\'POINT(\',latitude,\' \',longitude,\')\')) AS locA,
									CASE WHEN Azimuth-HBeam <0 
										THEN Round(360+(Azimuth-HBeam),1)
										ELSE Round(Azimuth-HBeam,1)
									END AS AzimuthMinusHBeam, 
									CASE WHEN Azimuth-HBeam/2 <0 
										THEN Round(360+(Azimuth-HBeam/2),1)
										ELSE Round(Azimuth-HBeam/2,1)
									END AS AzimuthMinusHalfHBeam, 
									Azimuth, 
									CASE WHEN Azimuth+HBeam/2>360
										THEN round(Azimuth+HBeam/2-360,1)
										ELSE round(Azimuth+HBeam/2,1)
									END AS AzimuthPlusHalfHBeam, 
									CASE WHEN Azimuth+HBeam>360
										THEN round(Azimuth+HBeam-360,1)
										ELSE round(Azimuth+HBeam,1)
									END AS AzimuthPlusHBeam, 
									HBeam,
									CASE WHEN Max_dist_2_cell*1.1 > 35 
										THEN 35000
										ELSE Max_dist_2_cell*1.1*1000
									END 
									AS DistLength,
									LenghtSector
								FROM (
									SELECT 	Site, CELLNAME, c.DlEarfcn AS Channel, longitude, latitude, 
										ROUND(MIN(`Best_dist_2_cell`),3) as Min_dist_2_cell, 
										ROUND(AVG(`Best_dist_2_cell`),3) as Mean_dist_2_cell,
										ROUND(MAX(`Best_dist_2_cell`),3) as Max_dist_2_cell,
										ROUND(MIN(`Best_RSRP`),1) as Min_RxLev, 
										ROUND(AVG(`Best_RSRP`),1) as Mean_RxLev,
										(CASE WHEN Azimuth REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN Azimuth ELSE 1 END) AS Azimuth,
										(CASE WHEN HBeam REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(HBeam,1) ELSE 60.0 END) AS HBeam,
										(CASE WHEN LenghtSector REGEXP \'^[0-9]+\\.?[0-9]*$\' THEN ROUND(LenghtSector,3) ELSE 1 END) AS LenghtSector										
									FROM ',@best4gTable, ' d
									LEFT OUTER JOIN operator.4g_cells_ready_archive c ON d.best_cellname = c.CELLNAME
									WHERE export_date = \'',@neededCellRefDate,'\' AND DlEarfcn = ',@freq4G,' 
									AND antenna NOT LIKE \'%738445%\' AND antenna NOT LIKE \'%738446%\' AND HBeam <> 360
									GROUP BY Cellname
									) as d
								) AS a
							) AS b'
					);

			PREPARE stmt FROM @t4;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @t5 = CONCAT('INSERT INTO ',@xCheckTable,' 
					SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, 2*HBeam as BeamSearch, DistSearch, 
						Min_dist_2_cell, Mean_dist_2_cell, Min_RxLev, Mean_RxLev,
						BestCellWithinBeamSearch_count, BestCellAllSamples_count, round(100*(1-BestCellWithinBeamSearch_count/BestCellAllSamples_count),1) AS CrossProbability_percent
						FROM (
							SELECT  c.Site, c.CELLNAME, c.Channel, c.Azimuth, c.HBeam, c.LenghtSector, c.DistSearch, 
							c.Min_dist_2_cell, c.Mean_dist_2_cell, c.Min_RxLev, c.Mean_RxLev,
								CASE WHEN e.BestCellWithinBeamSearch_count IS NOT NULL 
									THEN e.BestCellWithinBeamSearch_count
									ELSE 0 
								END AS BestCellWithinBeamSearch_count
							FROM (
								SELECT 	Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, COUNT(locPoint) AS BestCellWithinBeamSearch_count								
								FROM (
									SELECT Site, CELLNAME, Channel, Azimuth, HBeam, LenghtSector, DistSearch, locPoint
									FROM `temp_cellsforxcheck` c
									LEFT JOIN `temp_xcheck` t ON c.CELLNAME = t.Best_CellName
									WHERE ST_CONTAINS(PolyCellHBeam,locPoint)
									) AS d
								GROUP BY CELLNAME
								) as e
							RIGHT JOIN `temp_cellsforxcheck` c on e.CELLNAME = c.CELLNAME
							) as a
						LEFT JOIN (
								SELECT `Best_CellName`, COUNT(*) AS BestCellAllSamples_count
								FROM `temp_xcheck`
								GROUP BY `Best_CellName`
							) AS b ON a.CELLNAME = b.Best_CellName
						WHERE Min_dist_2_cell <= ',@min_dist,' AND BestCellAllSamples_count >4
						ORDER BY CrossProbability_percent desc'
					);
					
			PREPARE stmt FROM @t5;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		
		END IF;
	
	END WHILE;
END IF;

SET @ScanStreamName = (SELECT `Concat` FROM master_table WHERE NAME = scan_table);

DROP TABLE IF EXISTS temp_xcheckForMasterInsert;

SET @t6 = CONCAT('CREATE TEMPORARY TABLE temp_xcheckForMasterInsert
			SELECT * 
			FROM ',@xCheckTable,' 
			WHERE CrossProbability_percent>=50'
		);

PREPARE stmt FROM @t6;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF (SELECT COUNT(*) FROM temp_xcheckForMasterInsert) IS NOT NULL 
	THEN
	
	DELETE FROM xcheck_table WHERE Scan_stream = @ScanStreamName;
	INSERT INTO xcheck_table
		SELECT 	@ScanStreamName, `Site`, `CellName`,`Channel`, `Azimuth`, `HBeam`, `LenghtSector`, `BeamSearch`, `DistSearch`, 
			`Min_dist2cell`, `Mean_dist2cell`, `BestCellWithinBeamSearch_count`, `BestCellAllSamples_count`, `CrossProbability_percent`
		FROM temp_xcheckForMasterInsert
		ORDER BY Site, RIGHT(CellName,1), MID(CellName,5,1)
		;
	
END IF;	

END$$

DELIMITER ;