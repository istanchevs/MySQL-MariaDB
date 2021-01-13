DELIMITER $$

USE `operator_scanners`$$

DROP PROCEDURE IF EXISTS `ScanStat`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `ScanStat`(scan_table VARCHAR(100))

BEGIN

SET @summaryTable = CONCAT(scan_table,'_scan_summary');

# 2G statistics

# check if at least one 2G best server table exist

SET @t = CONCAT('SET @g = (SELECT table_name 
		FROM information_schema.tables 
		WHERE table_name LIKE \'',scan_table,'%\' AND table_name LIKE \'%gsm%\' AND table_name LIKE \'%bestserver\'
		limit 1)');
		
PREPARE stmt FROM @t;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF @g IS NOT NULL THEN
	
	# create 2g scanner table for RxLev statistics
	
	SET @coverage2gTable = CONCAT(scan_table,'_scan2g_rxlev');
	
	SET @t1=CONCAT('DROP TABLE IF EXISTS ',@coverage2gTable);
	
	PREPARE stmt FROM @t1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @t2=CONCAT(
		'CREATE TABLE ',@coverage2gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_RxLev VARCHAR(10), L_RxLev VARCHAR(10), 
							Range_RxLev VARCHAR(20), PRIMARY KEY (id))'
		);
	PREPARE stmt FROM @t2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# add ranges into RxLev hyst table
	
	SET @i = 1;
	WHILE @i<131 
		DO
		SET @r1=CONCAT('INSERT INTO ',@coverage2gTable,' (H_RxLev, L_RxLev, Range_RxLev)
				VALUES( (-9.5-',@i,'), (-10.5-',@i,'), 
					CONCAT((-9.5-',@i,'), \' to \',(-10.5-',@i,')))'
				);
		
		PREPARE stmt FROM @r1;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
	SET @i=@i+1;
	END WHILE;

	# create scanner table for summary statistics
	
	SET @s1=CONCAT('DROP TABLE IF EXISTS ',@summaryTable);
	
	PREPARE stmt FROM @s1;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @s2=CONCAT(
		'CREATE TABLE ',@summaryTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, Technology VARCHAR(10), Operator VARCHAR(20), Band VARCHAR(20), 
							Frequency VARCHAR(10),Bandwidth VARCHAR(20), KPI VARCHAR(20), Min DECIMAL(6,1), Avg DECIMAL(6,1), 
							50th_percentile DECIMAL(6,1), 95th_percentile DECIMAL(6,1), 
							Max DECIMAL(6,1), Total_counts INTEGER, PRIMARY KEY (id))'
		);
		
	PREPARE stmt FROM @s2;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	# create 2g scanner table for percentile statistics
			 
	SET @perc2gTable = CONCAT(scan_table,'_scan2g_perc');
			
	SET @t3=CONCAT('DROP TABLE IF EXISTS ',@perc2gTable);
			
	PREPARE stmt FROM @t3;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @t4=CONCAT(
		'CREATE TABLE ',@perc2gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))'
		);
			
	PREPARE stmt FROM @t4;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET @ops = ',op1_,op2_,';
	WHILE LOCATE(',',@ops) > 0
	DO
	
		SET @op = LEFT(@ops, LOCATE(',',@ops)-1);
		SET @ops = SUBSTRING(@ops, LOCATE(',',@ops) + 1);
		
		SET @best2gTable = CONCAT(scan_table, '_gsm_',@op, 'bestserver');
		
		SET @s = CONCAT('SET @b = (SELECT table_name FROM information_schema.tables WHERE table_name = \'',@best2gTable,'\')');
		
		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

		IF @b IS NOT NULL 
			THEN
				IF @op = '' THEN SET @op = 'operator';
					ELSEIF @op = 'op1_' THEN SET @op = 'op1';
					ELSEIF @op = 'op2_' THEN SET @op = 'op2';
				END IF; 
			
			# add counting column into RxLev table
			
			SET @t5=CONCAT('ALTER TABLE ',@coverage2gTable,
				' ADD Count_RxLev_',@op,' VARCHAR(10) AFTER Range_RxLev'
				);
				
			PREPARE stmt FROM @t5;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			#insert hyst count values into 2g RxLev table according to the ranges 
			SET @i = 1;
			WHILE @i<131 
				DO
				SET @t6=CONCAT('UPDATE ',@coverage2gTable,' a 
						INNER JOIN (SELECT COUNT(Best_Scanned_Level) AS Count_RxLev
								FROM ',@best2gTable,' 
								WHERE Best_Scanned_Level<(-9.5-',@i,') AND Best_Scanned_Level>=(-10.5-',@i,')
								) b
						SET Count_RxLev_',@op,' = b.Count_RxLev
						WHERE id=',@i
					);
					
				PREPARE stmt FROM @t6;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
				
				SET @i=@i+1;
			END WHILE;
							
			# add percentile columns per operator into percentile table
			
			SET @t7=CONCAT('ALTER TABLE ',@perc2gTable,
				' ADD Scanned_Level_',@op,' DOUBLE AFTER id,
				  ADD Percentile_',@op,' DECIMAL(6,3) AFTER Scanned_Level_',@op
				  );
				
			PREPARE stmt FROM @t7;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			# insert percentile statistics into the table
	
			SET @t8=CONCAT('INSERT INTO ',@perc2gTable,' (Scanned_Level_',@op,', Percentile_',@op,')
					SELECT DISTINCT  
						a.Best_Scanned_Level as Scanned_Level,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best2gTable,' AS b 
						WHERE b.Best_Scanned_Level <= a.Best_Scanned_Level ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best2gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best2gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt FROM @t8;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			# insert summary statistics into the table

			SET @t9=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 50th_percentile, 95th_percentile, MAX, Total_counts)
						SELECT 	\'2G\',
							@op,
							\'GSM\',
							\'900&1800\',
							\'n/a\',
							\'RxLev\',
							MIN(Best_Scanned_Level), 
							AVG(Best_Scanned_Level), 
								(SELECT Scanned_Level_',@op,' 
								FROM  ',@perc2gTable,'
								WHERE Percentile_',@op,'>=50
								LIMIT 1)  
							AS 50th_percentile, 
								(SELECT Scanned_Level_',@op,' 
								FROM  ',@perc2gTable,'
								WHERE Percentile_',@op,'>=95
								LIMIT 1) 
							AS 95th_percentile, 
							MAX(Best_Scanned_Level), 
								(SELECT COUNT(*)
								FROM ',@best2gTable,')
							AS Total_Counts
						FROM ',@best2gTable
					);
			
			PREPARE stmt FROM @t9;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			# insert BCCH&BSIC count statistics into the table

			SET @t10=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, Total_counts)
						VALUES (\'2G\',
							@op,
							\'GSM\',
							\'900&1800\',
							\'n/a\',
							\'Best Scanned cells\',
							(SELECT COUNT(BCCH_BSIC)
							FROM (
								SELECT CONCAT(ARFCN,"_",BSIC) AS BCCH_BSIC
								FROM ',@best2gTable,' 
								GROUP BY CONCAT(ARFCN,"_",BSIC)
								) AS a
							)
						)
					');
			
			PREPARE stmt FROM @t10;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END IF;
	END WHILE;
END IF; 

# 3G statistics

# check if summary table is already created 

IF @g IS NULL THEN
	# create scanner table for summary statistics
	
	SET @s1=CONCAT('DROP TABLE IF EXISTS ',@summaryTable);
	
	PREPARE stmt1 FROM @s1;
	EXECUTE stmt1;
	DEALLOCATE PREPARE stmt1;
	
	SET @s2=CONCAT(
		'CREATE TABLE ',@summaryTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, Technology VARCHAR(10), Operator VARCHAR(20), Band VARCHAR(20), 
							Frequency VARCHAR(10),Bandwidth VARCHAR(20), KPI VARCHAR(20), Min DECIMAL(6,1), Avg DECIMAL(6,1), 
							50th_percentile DECIMAL(6,1), 95th_percentile DECIMAL(6,1), 
							Max DECIMAL(6,1), Total_counts INTEGER, PRIMARY KEY (id))'
		);
		
	PREPARE stmt2 FROM @s2;
	EXECUTE stmt2;
	DEALLOCATE PREPARE stmt2;
	
END IF;

#Find best server tables and frequencies for scan statistics calculation
	
DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table, '_umts_bestserver%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables 
		WHERE table_name LIKE @searchName;
	
SET @freqs3G = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @freq3G = (SELECT SUBSTRING_INDEX(`names`, '_', -1) FROM tableNames WHERE id=@j);
	SET @freqs3G = (CONCAT(@freq3G,',',@freqs3G));
	SET @j = @j+1;
END WHILE;

SET @op = '';
SET @band = '';
SET @bw = '';

#SET @vivaFreqs3G = '10837,10812,10787,2981,2977,2958,';

SET @rscp3gTable = CONCAT(scan_table,'_scan3g_rscp');
SET @ecio3gTable = CONCAT(scan_table,'_scan3g_ecio');
SET @perc3gTable = CONCAT(scan_table,'_scan3g_perc');

# if at least one best server table exist then drop and re-create 3g scanner tables for statistics

SET @u = (SELECT table_name FROM information_schema.tables WHERE table_name LIKE @searchName LIMIT 1);
IF @u IS NOT NULL
	THEN
	SET @d1=CONCAT('DROP TABLE IF EXISTS ',@rscp3gTable);
	SET @d2=CONCAT('DROP TABLE IF EXISTS ',@ecio3gTable);
	SET @d3=CONCAT('DROP TABLE IF EXISTS ',@perc3gTable);	
						
	PREPARE stmtd1 FROM @d1;
	EXECUTE stmtd1;
	DEALLOCATE PREPARE stmtd1;
	
	PREPARE stmtd2 FROM @d2;
	EXECUTE stmtd2;
	DEALLOCATE PREPARE stmtd2;
	
	PREPARE stmtd3 FROM @d3;
	EXECUTE stmtd3;
	DEALLOCATE PREPARE stmtd3;
				
	SET @c1=CONCAT('CREATE TABLE ',@rscp3gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_RSCP VARCHAR(20), L_RSCP VARCHAR(20), Range_RSCP VARCHAR(20), PRIMARY KEY (id))');
	SET @c2=CONCAT('CREATE TABLE ',@ecio3gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_EcIo VARCHAR(20), L_EcIo VARCHAR(20), Range_EcIo VARCHAR(20), PRIMARY KEY (id))');
	SET @c3=CONCAT('CREATE TABLE ',@perc3gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))');

	PREPARE stmtc1 FROM @c1;
	EXECUTE stmtc1;
	DEALLOCATE PREPARE stmtc1;
	
	PREPARE stmtc2 FROM @c2;
	EXECUTE stmtc2;
	DEALLOCATE PREPARE stmtc2;
	
	PREPARE stmtc3 FROM @c3;
	EXECUTE stmtc3;
	DEALLOCATE PREPARE stmtc3;
	
	# add ranges into RSCP hyst table
	
	SET @i = 1;
	WHILE @i<131 
		DO
		SET @r1=CONCAT('INSERT INTO ',@rscp3gTable,' (H_RSCP, L_RSCP, Range_RSCP)
				VALUES( (-9.5-',@i,'), (-10.5-',@i,'), 
					CONCAT((-9.5-',@i,'), \' to \',(-10.5-',@i,')))'
				);
		
		PREPARE stmtr1 FROM @r1;
		EXECUTE stmtr1;
		DEALLOCATE PREPARE stmtr1;
	
	SET @i=@i+1;
	END WHILE;
	
	# add ranges into EcIo hyst table
	
	SET @i = 1;
	WHILE @i<32 
		DO
		SET @e1=CONCAT('INSERT INTO ',@ecio3gTable,' (H_EcIo, L_EcIo, Range_EcIo)
				VALUES( (1.5-',@i,'), (0.5-',@i,'), 
					CONCAT((1.5-',@i,'), \' to \',(0.5-',@i,')))'
				);
		
		PREPARE stmte1 FROM @e1;
		EXECUTE stmte1;
		DEALLOCATE PREPARE stmte1;
	
	SET @i=@i+1;
	END WHILE;	
	
	# insert statistics per frequency into tables
	WHILE (LOCATE(',', @freqs3G) > 0) 
	DO
		SET @freq3G = LEFT(@freqs3G, LOCATE(',',@freqs3G)-1);
		SET @freqs3G = SUBSTRING(@freqs3G, LOCATE(',',@freqs3G) + 1);	
	
		# check if best server table for selected frequency exist 
	
		SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(scan_table,'_umts_bestserver_',@freq3G));
		
		IF @bestServerExist IS NOT NULL 
			THEN
			SET @best3gTable = CONCAT(scan_table,'_umts_bestserver_',@freq3G);
			
			# add counting column into RSCP table
			
			SET @t3=CONCAT('ALTER TABLE ',@rscp3gTable,
				' ADD Count_RSCP_',@freq3G,' VARCHAR(20) AFTER Range_RSCP'
				);
				
			PREPARE stmt3 FROM @t3;
			EXECUTE stmt3;
			DEALLOCATE PREPARE stmt3;
			
			# insert values into RSCP table
			
			SET @i = 1;
			WHILE @i<131 
				DO
				SET @t4=CONCAT('UPDATE ',@rscp3gTable,' a 
						INNER JOIN (SELECT COUNT(RSCP_dBm) as Count
								FROM ',@best3gTable,' 
								WHERE RSCP_dBm < (-9.5-',@i,') AND RSCP_dBm >= (-10.5-',@i,')
								) b
						SET Count_RSCP_',@freq3G,' = b.Count
						WHERE id=',@i
					);
					
				PREPARE stmt4 FROM @t4;
				EXECUTE stmt4;
				DEALLOCATE PREPARE stmt4;
				
				SET @i=@i+1;
			END WHILE;
		
			# add counting column into EcIo table
			
			SET @t5=CONCAT('ALTER TABLE ',@ecio3gTable,
				' ADD Count_EcIo_',@freq3G,' VARCHAR(20) AFTER Range_EcIo'
				);
				
			PREPARE stmt5 FROM @t5;
			EXECUTE stmt5;
			DEALLOCATE PREPARE stmt5;
			
			# insert values into EcIo table
			
			SET @i = 1;
			WHILE @i<32 
				DO
				SET @t6=CONCAT('UPDATE ',@ecio3gTable,' a 
						INNER JOIN (SELECT COUNT(Best_EcIo) as Count
								FROM ',@best3gTable,' 
								WHERE Best_EcIo < (1.5-',@i,') AND Best_EcIo >= (0.5-',@i,')
								) b
						SET Count_EcIo_',@freq3G,' = b.Count
						WHERE id=',@i
					);
					
				PREPARE stmt6 FROM @t6;
				EXECUTE stmt6;
				DEALLOCATE PREPARE stmt6;
				
				SET @i=@i+1;
			END WHILE;
			
			# add percentile columns per frequency into percentile table
			
			SET @t7=CONCAT('ALTER TABLE ',@perc3gTable,
				' ADD Scanned_RSCP_',@freq3G,' DOUBLE AFTER id,
				  ADD Percentile_RSCP_',@freq3G,' DECIMAL(6,3) AFTER Scanned_RSCP_',@freq3G,',
				  ADD Scanned_EcIo_',@freq3G,' DOUBLE AFTER id,
				  ADD Percentile_EcIo_',@freq3G,' DECIMAL(6,3) AFTER Scanned_EcIo_',@freq3G
				  );
				
			PREPARE stmt7 FROM @t7;
			EXECUTE stmt7;
			DEALLOCATE PREPARE stmt7;
			
			# insert percentile statistics into the table
	
			SET @t8=CONCAT('INSERT INTO ',@perc3gTable,' (Scanned_RSCP_',@freq3G,', Percentile_RSCP_',@freq3G,')
					SELECT DISTINCT  
						a.RSCP_dBm,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best3gTable,' AS b 
						WHERE b.RSCP_dBm <= a.RSCP_dBm ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best3gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best3gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt8 FROM @t8;
			EXECUTE stmt8;
			DEALLOCATE PREPARE stmt8;
			
			SET @t8=CONCAT('INSERT INTO ',@perc3gTable,' (Scanned_EcIo_',@freq3G,', Percentile_EcIo_',@freq3G,')
					SELECT DISTINCT  
						a.Best_EcIo,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best3gTable,' AS b 
						WHERE b.Best_EcIo <= a.Best_EcIo ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best3gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best3gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt8 FROM @t8;
			EXECUTE stmt8;
			DEALLOCATE PREPARE stmt8;
			
			# find the band according to the information table
			
			IF @freq3G 		BETWEEN 2937 AND 3088 THEN 	SET @band = '900';
				ELSEIF @freq3G 	BETWEEN 1162 AND 1513 THEN 	SET @band = '1800';
				ELSEIF @freq3G 	BETWEEN 10562 AND 10838 THEN 	SET @band = '2100';
				ELSEIF @freq3G 	BETWEEN 2237 AND 2563 THEN 	SET @band = '2600';
				ELSE 						SET @band = 'n/a';
			END IF;	
			
			# find operator name according the scanned frequency

			IF (@freq3G BETWEEN 10750 AND 10850) OR (@freq3G BETWEEN 2946 AND 3001) THEN 			SET @op = 'operator';
				ELSEIF (@freq3G BETWEEN 10650 AND 10749) OR (@freq3G BETWEEN 3004 AND 3050) THEN 	SET @op = 'op1';
				ELSEIF (@freq3G BETWEEN 10562 AND 10649) OR (@freq3G BETWEEN 3053 AND 3099) THEN 	SET @op = 'op2';
				ELSE 											SET @op = 'n/a';
			END IF;
			
			# insert summary statistics into the table
			
			SET @t9=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 50th_percentile, 95th_percentile, MAX, Total_counts)
				SELECT 	\'3G\',
					@op,
					@band,',
					@freq3G,',
					\'5.000MHz\',
					\'RSCP\',
					MIN(RSCP_dBm), 
					AVG(RSCP_dBm), 
						(SELECT Scanned_RSCP_',@freq3G,'
						FROM  ',@perc3gTable,'
						WHERE Percentile_RSCP_',@freq3G,'>=50
						LIMIT 1)  
					AS 50th_percentile, 
						(SELECT Scanned_RSCP_',@freq3G,'
						FROM  ',@perc3gTable,'
						WHERE Percentile_RSCP_',@freq3G,'>=95
						LIMIT 1) 
					AS 95th_percentile, 
					MAX(RSCP_dBm), 
						(SELECT COUNT(*)
						FROM ',@best3gTable,')
					AS Total_Counts
				FROM ',@best3gTable
			);
			
			PREPARE stmt9 FROM @t9;
			EXECUTE stmt9;
			DEALLOCATE PREPARE stmt9;
			
			SET @t10=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 50th_percentile, 95th_percentile, MAX, Total_counts)
				SELECT 	\'3G\',
					@op,
					@band,',
					@freq3G,',
					\'5.000MHz\',
					\'EcIo\',
					MIN(Best_EcIo), 
					AVG(Best_EcIo), 
						(SELECT Scanned_EcIo_',@freq3G,'
						FROM  ',@perc3gTable,'
						WHERE Percentile_EcIo_',@freq3G,'>=50
						LIMIT 1)  
					AS 50th_percentile, 
						(SELECT Scanned_EcIo_',@freq3G,'
						FROM  ',@perc3gTable,'
						WHERE Percentile_EcIo_',@freq3G,'>=95
						LIMIT 1) 
					AS 95th_percentile, 
					MAX(Best_EcIo), 
						(SELECT COUNT(*)
						FROM ',@best3gTable,')
					AS Total_Counts
				FROM ',@best3gTable
			);
	
			PREPARE stmt10 FROM @t10;
			EXECUTE stmt10;
			DEALLOCATE PREPARE stmt10;
			
			# insert PSC count statistics into the table

			SET @t11=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, Total_counts)
						VALUES (\'3G\',
							@op,
							@band,',
							@freq3G,',
							\'5.000MHz\',
							\'Best Scanned PSCs\',
							(SELECT COUNT(PSC)
							FROM (
								SELECT PSC
								FROM ',@best3gTable,' 
								GROUP BY PSC
								) AS a
							)
						)
					');
			
			PREPARE stmt FROM @t11;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
		END IF;
	
	END WHILE;
END IF;


# 4G statistics

# check if summary table is already created 
IF @g IS NULL THEN
	IF @u IS NULL THEN
		# create scanner table for summary statistics
	
		SET @s1=CONCAT('DROP TABLE IF EXISTS ',@summaryTable);
		
		PREPARE stmt1 FROM @s1;
		EXECUTE stmt1;
		DEALLOCATE PREPARE stmt1;
		
	SET @s2=CONCAT(
		'CREATE TABLE ',@summaryTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, Technology VARCHAR(10), Operator VARCHAR(20), Band VARCHAR(20), 
							Frequency VARCHAR(10),Bandwidth VARCHAR(20), KPI VARCHAR(20), Min DECIMAL(6,1), Avg DECIMAL(6,1), 
							50th_percentile DECIMAL(6,1), 95th_percentile DECIMAL(6,1), 
							Max DECIMAL(6,1), Total_counts INTEGER, PRIMARY KEY (id))'
		);
		
		PREPARE stmt2 FROM @s2;
		EXECUTE stmt2;
		DEALLOCATE PREPARE stmt2;
	
	END IF;
END IF;

DROP TABLE IF EXISTS tableNames;
CREATE TEMPORARY TABLE tableNames (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     `names` TEXT NOT NULL, PRIMARY KEY (id));
     
SET @searchName = CONCAT(scan_table, '_lte_bestserver%');
INSERT INTO tableNames (`names`)
	SELECT table_name FROM information_schema.tables 
		WHERE table_name LIKE @searchName;
	
SET @freqs4G = '';
SET @j = 1;	
SET @i = (SELECT COUNT(*) FROM tableNames);
WHILE @j<=@i DO
	SET @freq4G = (SELECT SUBSTRING_INDEX(`names`, '_', -1) FROM tableNames WHERE id=@j);
	SET @freqs4G = (CONCAT(@freq4G,',',@freqs4G));
	SET @j = @j+1;
END WHILE;

SET @op = '';
SET @band = '';
SET @bw = '';
#SET @vivaFreqs4G = '550,575,1575,3516,';

SET @rsrp4gTable = CONCAT(scan_table,'_scan4g_rsrp');
SET @rsrq4gTable = CONCAT(scan_table,'_scan4g_rsrq');
SET @cinr4gTable = CONCAT(scan_table,'_scan4g_cinr');
SET @perc4gTable = CONCAT(scan_table,'_scan4g_perc');

# if at least one best server table exist then drop and re-create 4g scanner tables for statistics

SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name LIKE CONCAT(scan_table,'_lte_bestserver%') LIMIT 1);
IF @bestServerExist IS NOT NULL
	THEN
	SET @d1=CONCAT('DROP TABLE IF EXISTS ',@rsrp4gTable);
	SET @d2=CONCAT('DROP TABLE IF EXISTS ',@rsrq4gTable);
	SET @d3=CONCAT('DROP TABLE IF EXISTS ',@cinr4gTable);
	SET @d4=CONCAT('DROP TABLE IF EXISTS ',@perc4gTable);	
						
	PREPARE stmtd1 FROM @d1;
	EXECUTE stmtd1;
	DEALLOCATE PREPARE stmtd1;
	
	PREPARE stmtd2 FROM @d2;
	EXECUTE stmtd2;
	DEALLOCATE PREPARE stmtd2;
	
	PREPARE stmtd3 FROM @d3;
	EXECUTE stmtd3;
	DEALLOCATE PREPARE stmtd3;
	
	PREPARE stmtd4 FROM @d4;
	EXECUTE stmtd4;
	DEALLOCATE PREPARE stmtd4;
							
	SET @c1=CONCAT('CREATE TABLE ',@rsrp4gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_RSRP VARCHAR(20), L_RSRP VARCHAR(20), Range_RSRP VARCHAR(20), PRIMARY KEY (id))');
	SET @c2=CONCAT('CREATE TABLE ',@rsrq4gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_RSRQ VARCHAR(20), L_RSRQ VARCHAR(20), Range_RSRQ VARCHAR(20), PRIMARY KEY (id))');
	SET @c3=CONCAT('CREATE TABLE ',@cinr4gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, H_CINR VARCHAR(20), L_CINR VARCHAR(20), Range_CINR VARCHAR(20), PRIMARY KEY (id))');	
	SET @c4=CONCAT('CREATE TABLE ',@perc4gTable,' (id MEDIUMINT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))');

	PREPARE stmtc1 FROM @c1;
	EXECUTE stmtc1;
	DEALLOCATE PREPARE stmtc1;
	
	PREPARE stmtc2 FROM @c2;
	EXECUTE stmtc2;
	DEALLOCATE PREPARE stmtc2;
	
	PREPARE stmtc3 FROM @c3;
	EXECUTE stmtc3;
	DEALLOCATE PREPARE stmtc3;
	
	PREPARE stmtc4 FROM @c4;
	EXECUTE stmtc4;
	DEALLOCATE PREPARE stmtc4;
	
	# add ranges into RSRP hyst table
	
	SET @i = 1;
	WHILE @i<131 
		DO
		SET @r1=CONCAT('INSERT INTO ',@rsrp4gTable,' (H_RSRP,L_RSRP, Range_RSRP)
				VALUES( (-9.5-',@i,'), (-10.5-',@i,'), 
					CONCAT((-9.5-',@i,'), \' to \',(-10.5-',@i,')))'
				);
		
		PREPARE stmtr1 FROM @r1;
		EXECUTE stmtr1;
		DEALLOCATE PREPARE stmtr1;
	
	SET @i=@i+1;
	END WHILE;
	
	# add ranges into RSRQ hyst table
	
	SET @i = 1;
	WHILE @i<32 
		DO
		SET @e1=CONCAT('INSERT INTO ',@rsrq4gTable,' (H_RSRQ, L_RSRQ, Range_RSRQ)
				VALUES( (1.5-',@i,'), (0.5-',@i,'), 
					CONCAT((1.5-',@i,'), \' to \',(0.5-',@i,')))'
				);
		
		PREPARE stmte1 FROM @e1;
		EXECUTE stmte1;
		DEALLOCATE PREPARE stmte1;
	
	SET @i=@i+1;
	END WHILE;	

	# add ranges into CINR hyst table
	
	SET @i = 1;
	WHILE @i<72 
		DO
		SET @c1=CONCAT('INSERT INTO ',@cinr4gTable,' (H_CINR, L_CINR, Range_CINR)
				VALUES( (42.5-',@i,'), (41.5-',@i,'), 
					CONCAT((42.5-',@i,'), \' to \',(41.5-',@i,')))'
				);
		
		PREPARE stmtc1 FROM @c1;
		EXECUTE stmtc1;
		DEALLOCATE PREPARE stmtc1;
	
	SET @i=@i+1;
	END WHILE;
	
	# create temp table with channels and measured bandwidth
	
	DROP TABLE IF EXISTS temp_chanbw;
	
	SET @rootTable = CONCAT(scan_table, '_lte');	
	SET @r = CONCAT('CREATE TEMPORARY TABLE temp_chanbw
				SELECT `EARFCN`, ROUND(AVG(`DL Bandwidth`)*0.2,3) AS BW_MHz
				FROM ',@rootTable,' 
				GROUP BY `EARFCN`');
				
	PREPARE stmt FROM @r;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
	
	# insert statistics per frequency into tables
			
	WHILE (LOCATE(',', @freqs4G) > 0) 
	DO
		SET @freq4G = LEFT(@freqs4G, LOCATE(',',@freqs4G)-1);
		SET @freqs4G = SUBSTRING(@freqs4G, LOCATE(',',@freqs4G) + 1);	
	
		# check if best server table for selected frequency exist 
	
		SET @bestServerExist = (SELECT table_name FROM information_schema.tables WHERE table_name = CONCAT(scan_table,'_lte_bestserver_',@freq4G));
		IF @bestServerExist IS NOT NULL 
			THEN
			SET @best4gTable = CONCAT(scan_table,'_lte_bestserver_',@freq4G);
			
			# add counting column into RSRP table
			
			SET @t3=CONCAT('ALTER TABLE ',@rsrp4gTable,
				' ADD Count_RSRP_',@freq4G,' VARCHAR(20) AFTER Range_RSRP'
				);
				
			PREPARE stmt3 FROM @t3;
			EXECUTE stmt3;
			DEALLOCATE PREPARE stmt3;
			
			# insert values into RSRP table
			
			SET @i = 1;
			WHILE @i<131 
				DO
				SET @t4=CONCAT('UPDATE ',@rsrp4gTable,' a 
						INNER JOIN (SELECT COUNT(RSRP) as Count
								FROM ',@best4gTable,' 
								WHERE RSRP <(-9.5-',@i,') AND RSRP >= (-10.5-',@i,')
								) b
						SET Count_RSRP_',@freq4G,' = b.Count
						WHERE id=',@i
					);
					
				PREPARE stmt4 FROM @t4;
				EXECUTE stmt4;
				DEALLOCATE PREPARE stmt4;
				
				SET @i=@i+1;
			END WHILE;
		
			# add counting column into RSRQ table
			
			SET @t5=CONCAT('ALTER TABLE ',@rsrq4gTable,
				' ADD Count_RSRQ_',@freq4G,' VARCHAR(20) AFTER Range_RSRQ'
				);
				
			PREPARE stmt5 FROM @t5;
			EXECUTE stmt5;
			DEALLOCATE PREPARE stmt5;
			
			# insert values into RSRQ table
			
			SET @i = 1;
			WHILE @i<32 
				DO
				SET @t6=CONCAT('UPDATE ',@rsrq4gTable,' a 
						INNER JOIN (SELECT COUNT(RSRQ) as Count
								FROM ',@best4gTable,' 
								WHERE RSRQ <(1.5-',@i,') AND RSRQ >= (0.5-',@i,')
								) b
						SET Count_RSRQ_',@freq4G,' = b.Count
						WHERE id=',@i
					);
					
				PREPARE stmt6 FROM @t6;
				EXECUTE stmt6;
				DEALLOCATE PREPARE stmt6;
				
				SET @i=@i+1;
			END WHILE;
			
			# add counting column into CINR table
			
			SET @t7=CONCAT('ALTER TABLE ',@cinr4gTable,
				' ADD Count_CINR_',@freq4G,' VARCHAR(20) AFTER Range_CINR'
				);
				
			PREPARE stmt7 FROM @t7;
			EXECUTE stmt7;
			DEALLOCATE PREPARE stmt7;
			
			# insert values into CINR table
			
			SET @i = 1;
			WHILE @i<72
				DO
				SET @t8=CONCAT('UPDATE ',@cinr4gTable,' a 
						INNER JOIN (SELECT COUNT(CINR) as Count
								FROM ',@best4gTable,' 
								WHERE CINR <(42.5-',@i,') AND CINR >= (41.5-',@i,')
								) b
						SET Count_CINR_',@freq4G,' = b.Count
						WHERE id=',@i
					);
					
				PREPARE stmt8 FROM @t8;
				EXECUTE stmt8;
				DEALLOCATE PREPARE stmt8;
				
				SET @i=@i+1;
			END WHILE;			
			
			# add percentile columns per frequency into percentile table
			
			SET @t9=CONCAT('ALTER TABLE ',@perc4gTable,
				' ADD Scanned_RSRP_',@freq4G,' DOUBLE AFTER id,
				  ADD Percentile_RSRP_',@freq4G,' DECIMAL(6,3) AFTER Scanned_RSRP_',@freq4G,',
				  ADD Scanned_RSRQ_',@freq4G,' DOUBLE AFTER Percentile_RSRP_',@freq4G,',
				  ADD Percentile_RSRQ_',@freq4G,' DECIMAL(6,3) AFTER Scanned_RSRQ_',@freq4G,',
				  ADD Scanned_CINR_',@freq4G,' DOUBLE AFTER Percentile_RSRQ_',@freq4G,',
				  ADD Percentile_CINR_',@freq4G,' DECIMAL(6,3) AFTER Scanned_CINR_',@freq4G				  
				  );
				
			PREPARE stmt9 FROM @t9;
			EXECUTE stmt9;
			DEALLOCATE PREPARE stmt9;
			
			# insert percentile statistics into the table
	
			SET @t10=CONCAT('INSERT INTO ',@perc4gTable,' (Scanned_RSRP_',@freq4G,', Percentile_RSRP_',@freq4G,')
					SELECT DISTINCT  
						a.RSRP,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best4gTable,' AS b 
						WHERE b.RSRP <= a.RSRP ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best4gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best4gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt10 FROM @t10;
			EXECUTE stmt10;
			DEALLOCATE PREPARE stmt10;
			
			SET @t11=CONCAT('INSERT INTO ',@perc4gTable,' (Scanned_RSRQ_',@freq4G,', Percentile_RSRQ_',@freq4G,')
					SELECT DISTINCT  
						a.RSRQ,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best4gTable,' AS b 
						WHERE b.RSRQ <= a.RSRQ ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best4gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best4gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt11 FROM @t11;
			EXECUTE stmt11;
			DEALLOCATE PREPARE stmt11;			

			SET @t12=CONCAT('INSERT INTO ',@perc4gTable,' (Scanned_CINR_',@freq4G,', Percentile_CINR_',@freq4G,')
					SELECT DISTINCT  
						a.CINR,
						ROUND( 100.0 * (1-( SELECT COUNT(*) FROM ',@best4gTable,' AS b 
						WHERE b.CINR <= a.CINR ) / total.cnt), 3 ) 
						AS percentile
					FROM ',@best4gTable,' a  
					CROSS JOIN (  
						SELECT COUNT(*) AS cnt  
						FROM ',@best4gTable,' 
						) AS total 
					ORDER BY Percentile ASC'
					);
	
			PREPARE stmt12 FROM @t12;
			EXECUTE stmt12;
			DEALLOCATE PREPARE stmt12;
			
			# find the band according to the information table
			
			IF @freq4G 		BETWEEN 3450 AND 3799 	THEN 	SET @band = '900';
				ELSEIF @freq4G 	BETWEEN 1200 AND 1949 	THEN 	SET @band = '1800';
				ELSEIF @freq4G 	BETWEEN 0 AND 599 	THEN 	SET @band = '2100';
				ELSEIF @freq4G 	BETWEEN 2750 AND 3499 	THEN 	SET @band = '2600';
				ELSE 						SET @band = 'n/a';
			END IF;			
			
			# find operator name and bandwidth
			IF (@freq4G BETWEEN 3490 AND 3602) OR (@freq4G BETWEEN 1502 AND 1650) OR (@freq4G BETWEEN 400 AND 600) OR (@freq4G BETWEEN 3050 AND 3248) 
				THEN 	SET @op = 'operator';
					/*
					IF @freq4G = '3516' THEN SET @bw = '3MHz';
						ELSEIF @freq4G = '1575' THEN SET @bw = '15MHz';
						ELSEIF @freq4G = '500' THEN SET @bw = '20MHz';
						ELSEIF @freq4G = '525' THEN SET @bw = '15MHz';
						ELSEIF @freq4G = '550' THEN SET @bw = '10MHz';
						ELSEIF @freq4G = '575' THEN SET @bw = '5MHz';
						ELSEIF @freq4G = '3150' THEN SET @bw = '20MHz';
						ELSE SET @bw = 'n/a';
					END IF;
					*/
				ELSEIF (@freq4G BETWEEN 3608 AND 3700) OR (@freq4G BETWEEN 1202 AND 1350) OR (@freq4G BETWEEN 200 AND 398) OR (@freq4G BETWEEN 2850 AND 3048) 
					THEN 	SET @op = 'op1';
						/*
						IF @freq4G = '3622' THEN SET @bw = '3MHz'; 
							ELSEIF @freq4G = '3678' THEN SET @bw = '5MHz';
							ELSEIF @freq4G = '1275' THEN SET @bw = '15MHz';
							ELSEIF @freq4G = '250' THEN SET @bw = '10MHz';
							ELSEIF @freq4G = '275' THEN SET @bw = '15MHz';
							ELSEIF @freq4G = '300' THEN SET @bw = '20MHz';
							ELSEIF @freq4G = '325' THEN SET @bw = '5MHz';
							ELSEIF @freq4G = '350' THEN SET @bw = '10MHz';
							ELSEIF @freq4G = '375' THEN SET @bw = '5MHz';
							ELSEIF @freq4G = '2950' THEN SET @bw = '20MHz';
							ELSE SET @bw = 'n/a';
						END IF;
						*/		
				ELSEIF (@freq4G BETWEEN 3706 AND 3798) OR (@freq4G BETWEEN 1352 AND 1500) OR (@freq4G BETWEEN 0 AND 198) OR (@freq4G BETWEEN 3250 AND 3448)
					THEN 	SET @op = 'op2';
						/*
						IF @freq4G = '3774' THEN SET @bw = '3/5MHz';
							ELSEIF @freq4G = '3780' THEN SET @bw = '3MHz';
							ELSEIF @freq4G = '1425' THEN SET @bw = '15MHz';
							ELSEIF @freq4G = '50' THEN SET @bw = '10MHz';
							ELSEIF @freq4G = '75' THEN SET @bw = '15MHz';
							ELSEIF @freq4G = '100' THEN SET @bw = '20MHz';
							ELSEIF @freq4G = '125' THEN SET @bw = '5MHz';
							ELSEIF @freq4G = '150' THEN SET @bw = '10MHz';
							ELSEIF @freq4G = '175' THEN SET @bw = '5MHz';
							ELSEIF @freq4G = '3350' THEN SET @bw = '20MHz';
							ELSE SET @bw = 'n/a';
						END IF;
						*/		
				ELSEIF @freq4G BETWEEN 1802 AND 1948
					THEN 	SET @op = 'Max';
						/*
						IF @freq4G = '1877' THEN SET @bw = '10MHz';
							ELSEIF @freq4G = '1875' THEN SET @bw = '15MHz';
							ELSE SET @bw = 'n/a';
						END IF;
						*/
				ELSEIF @freq4G BETWEEN 1652 AND 1726
					THEN 	SET @op = 'Bulsatcom';
						/*
						IF @freq4G = '1702' THEN SET @bw = '5MHz';
							ELSE SET @bw = 'n/a';
						END IF;
						*/
				ELSE 	SET @op = 'n/a';
					/*
					SET @bw = 'n/a';
					*/
			END IF;
			
			SET @r2 = CONCAT('set @measbw = (SELECT BW_MHz FROM temp_chanbw WHERE EARFCN = ',@freq4G,');');
			
			PREPARE stmt FROM @r2;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			# insert summary statistics into the table
			
			SET @bw = CONCAT(@measbw,'MHz');
			
			SET @t13=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 
									50th_percentile, 95th_percentile, MAX, Total_counts)
				SELECT 	\'4G\',
					@op,
					@band,',
					@freq4G,',
					@bw,
					\'RSRP\',
					MIN(RSRP), 
					AVG(RSRP), 
						(SELECT Scanned_RSRP_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_RSRP_',@freq4G,'>=50
						LIMIT 1)  
					AS 50th_percentile, 
						(SELECT Scanned_RSRP_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_RSRP_',@freq4G,'>=95
						LIMIT 1) 
					AS 95th_percentile, 
					MAX(RSRP), 
						(SELECT COUNT(*)
						FROM ',@best4gTable,')
					AS Total_Counts
				FROM ',@best4gTable
			);
			
			PREPARE stmt13 FROM @t13;
			EXECUTE stmt13;
			DEALLOCATE PREPARE stmt13;

			SET @t14=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 
									50th_percentile, 95th_percentile, MAX, Total_counts)
				SELECT 	\'4G\',
					@op,
					@band,',
					@freq4G,',
					@bw,
					\'RSRQ\',
					MIN(RSRQ), 
					AVG(RSRQ), 
						(SELECT Scanned_RSRQ_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_RSRQ_',@freq4G,'>=50
						LIMIT 1)  
					AS 50th_percentile, 
						(SELECT Scanned_RSRQ_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_RSRQ_',@freq4G,'>=95
						LIMIT 1) 
					AS 95th_percentile, 
					MAX(RSRQ), 
						(SELECT COUNT(*)
						FROM ',@best4gTable,')
					AS Total_Counts
				FROM ',@best4gTable
			);
			
			PREPARE stmt14 FROM @t14;
			EXECUTE stmt14;
			DEALLOCATE PREPARE stmt14;

			SET @t15=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, MIN, AVG, 
									50th_percentile, 95th_percentile, MAX, Total_counts)
				SELECT 	\'4G\',
					@op,
					@band,',
					@freq4G,',
					@bw,
					\'CINR\',
					MIN(CINR), 
					AVG(CINR), 
						(SELECT Scanned_CINR_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_CINR_',@freq4G,'>=50
						LIMIT 1)  
					AS 50th_percentile, 
						(SELECT Scanned_CINR_',@freq4G,'
						FROM  ',@perc4gTable,'
						WHERE Percentile_CINR_',@freq4G,'>=95
						LIMIT 1) 
					AS 95th_percentile, 
					MAX(CINR), 
						(SELECT COUNT(*)
						FROM ',@best4gTable,')
					AS Total_Counts
				FROM ',@best4gTable
			);
			
			PREPARE stmt15 FROM @t15;
			EXECUTE stmt15;
			DEALLOCATE PREPARE stmt15;
		
			# insert PCI count statistics into the table

			SET @t12=CONCAT('INSERT INTO ',@summaryTable,' (Technology, Operator, Band, Frequency, Bandwidth, KPI, Total_counts)
						VALUES (\'4G\',
							@op,
							@band,',
							@freq4G,',
							@bw,
							\'Best Scanned PCIs\',
							(SELECT COUNT(PCI)
							FROM (
								SELECT PCI
								FROM ',@best4gTable,' 
								GROUP BY PCI
								) AS a
							)
						)
					');
			
			PREPARE stmt FROM @t12;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
		END IF;
	
	END WHILE;
END IF;

END$$

DELIMITER ;	