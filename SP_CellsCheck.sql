DELIMITER $$

USE `operator`$$

DROP PROCEDURE IF EXISTS `cellsCheck`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `cellsCheck`()

BEGIN

# Check 2G parameters

DROP TABLE IF EXISTS temp1;
DROP TABLE IF EXISTS temp2;
DROP TABLE IF EXISTS temp3;
DROP TABLE IF EXISTS temp_trx;
DROP TABLE IF EXISTS CHECK_2G_Param;

	# Check 2G Hopping types and Quality Limits
	
CREATE TEMPORARY TABLE temp_trx(INDEX CELLNAME (`CELLNAME`))			
	SELECT t.CELLNAME, COUNT(TRXID) AS TRX_Count 
	FROM gtrx t 
	WHERE ACTSTATUS='ACTIVATED' 	
	GROUP BY CELLNAME;	
	
CREATE TEMPORARY TABLE temp1
	SELECT BSCName, CELLNAME,
		CASE WHEN CAST(freq AS UNSIGNED) BETWEEN 1 AND 27 THEN "YES"
		END
		AS P_GSM,
		CASE WHEN CAST(freq AS UNSIGNED) BETWEEN 900 AND 1024 THEN "YES"
		END
		AS E_GSM,
		CASE WHEN CAST(freq AS UNSIGNED) BETWEEN 125 AND 900 THEN "YES"
		END
		AS DCS
		FROM gtrx
		WHERE ACTSTATUS="ACTIVATED" 
		ORDER BY BSCName, CELLNAME;
		

CREATE TEMPORARY TABLE temp2
	SELECT BSCName, CELLNAME, COUNT(P_GSM) AS Count_PGSM_TRX, COUNT(E_GSM) AS Count_EGSM_TRX, COUNT(DCS) AS Count_DCS_TRX 
		FROM temp1
		GROUP BY CELLNAME ORDER BY BSCName, CELLNAME;
/*
CREATE TABLE CHECK_2G_Param	
	SELECT `City Cluster`, BSCName, SiteID, Site_Name, Site, CellID, CELLNAME, TCH, TRX_Count, HOPMODE, HSN, MAIO, 
		Count_PGSM_TRX, Count_EGSM_TRX, Count_DCS_TRX, DLQUALIMIT, ULQUALIMIT, DLQUALIMITAMRFR, ULQUALIMITAMRFR, DLQUALIMITAMRHR, ULQUALIMITAMRHR, 
		ACTSTATUS, CHECK_BBH_activation, CHECK_QualLimits
	FROM 	(
		SELECT `City Cluster`, G.BSCName, G.SiteID, G.Site_Name, G.Site, G.CellID, G.CELLNAME, TCH, g.TRX_Count, HOPMODE, HSN, MAIO, Count_PGSM_TRX, Count_EGSM_TRX, Count_DCS_TRX,
			DLQUALIMIT, ULQUALIMIT, DLQUALIMITAMRFR, ULQUALIMITAMRFR, DLQUALIMITAMRHR, ULQUALIMITAMRHR, ACTSTATUS,
			CASE 	WHEN r.TRX_Count > 1 AND HOPMODE IS NULL AND (Count_PGSM_TRX>1 OR Count_EGSM_TRX>1 OR Count_DCS_TRX>1) THEN "NOK"
				WHEN r.TRX_Count > 3 AND LOCATE("Baseband_FH", HOPMODE) > 0 AND Count_DCS_TRX>1 THEN "Check"
				WHEN r.TRX_Count = 1 THEN "1TRX"
				WHEN LOCATE("RF_FH", HOPMODE) > 0 THEN "SFH"
				ELSE "OK"
			END
			AS CHECK_BBH_activation,
			CASE 	WHEN LOCATE("Baseband_FH", HOPMODE) > 0 AND LOCATE("RF_FH", HOPMODE) > 0
					THEN "BBH&SFH"
			
				WHEN LOCATE("RF_FH", HOPMODE) > 0
					THEN 	(CASE 	WHEN DLQUALIMIT<55 OR ULQUALIMIT<55 OR DLQUALIMITAMRFR<55 OR DLQUALIMITAMRHR<55 OR ULQUALIMITAMRFR<55 OR ULQUALIMITAMRHR<55
							THEN "NOK_SFH<55"
							ELSE "OK"
						END)
						
				WHEN HOPMODE IS NULL OR LOCATE("Baseband_FH", HOPMODE) > 0
					THEN 	(CASE 	WHEN DLQUALIMIT>35 OR ULQUALIMIT>35 OR DLQUALIMITAMRFR>35 OR DLQUALIMITAMRHR>35 OR ULQUALIMITAMRFR>35 OR ULQUALIMITAMRHR>35
							THEN "NOK_NoHopOrBBH>35"
							ELSE "OK"
						END)
						
			#	WHEN HOPMODE IS NULL 
			#		THEN 	(CASE 	WHEN DLQUALIMIT>40 OR ULQUALIMIT>40 OR DLQUALIMITAMRFR>40 OR DLQUALIMITAMRHR>40 OR ULQUALIMITAMRFR>40 OR ULQUALIMITAMRHR>40
			#				THEN "NOK_NoHop>40"
			#				ELSE "OK"
			#			END)
			#	WHEN HOPMODE = "BaseBand_FH" 
			#		THEN 	(CASE 	WHEN DLQUALIMIT<40 OR ULQUALIMIT<40 OR DLQUALIMITAMRFR<40 OR DLQUALIMITAMRHR<40 OR ULQUALIMITAMRFR<40 OR ULQUALIMITAMRHR<40
			#				THEN "NOK_BBH<40"
			#				ELSE "OK"
			#			END)
				ELSE "N/A"
			END
				AS CHECK_QualLimits
		FROM 2g_cells_ready G
			LEFT OUTER JOIN batches B ON G.`Site` = B.`Site`
			LEFT OUTER JOIN temp2 T ON G.CELLNAME = T.CELLNAME
			LEFT OUTER JOIN temp_trx r ON G.CELLNAME = r.CELLNAME
			LEFT OUTER JOIN gcellhoemg E ON G.CELLNAME = E.CELLNAME
			LEFT OUTER JOIN gcellamrqul A ON G.CELLNAME = A.CELLNAME
		) AS a
	WHERE ACTSTATUS = 'ACTIVATED' AND (CHECK_BBH_activation = 'NOK' OR CHECK_QualLimits LIKE 'NOK%') ;
*/


CREATE TABLE CHECK_2G_Param	
	SELECT `City Cluster`, BSCName, SiteID, Site_Name, Site, CellID, CELLNAME, TCH, TRX_Count, HOPMODE, HSN, MAIO, 
		Count_PGSM_TRX, Count_EGSM_TRX, Count_DCS_TRX, DLQUALIMIT, ULQUALIMIT, DLQUALIMITAMRFR, ULQUALIMITAMRFR, DLQUALIMITAMRHR, ULQUALIMITAMRHR, 
		ACTSTATUS, CHECK_BBH_activation, CHECK_QualLimits
	FROM 	(
		SELECT `City Cluster`, G.BSCName, G.SiteID, G.Site_Name, G.Site, G.CellID, G.CELLNAME, TCH, g.TRX_Count, HOPMODE, HSN, MAIO, Count_PGSM_TRX, Count_EGSM_TRX, Count_DCS_TRX,
			DLQUALIMIT, ULQUALIMIT, DLQUALIMITAMRFR, ULQUALIMITAMRFR, DLQUALIMITAMRHR, ULQUALIMITAMRHR, ACTSTATUS,
			CASE 	WHEN r.TRX_Count > 1 AND HOPMODE IS NULL AND (Count_PGSM_TRX>1 OR Count_EGSM_TRX>1 OR Count_DCS_TRX>1) THEN "NOK"
				WHEN r.TRX_Count > 3 AND LOCATE("Baseband_FH", HOPMODE) > 0 AND Count_DCS_TRX>1 THEN "Check"
				WHEN r.TRX_Count = 1 THEN "1TRX"
				WHEN LOCATE("RF_FH", HOPMODE) > 0 THEN "SFH"
				ELSE "OK"
			END
			AS CHECK_BBH_activation,
			CASE 	WHEN DLQUALIMIT<55 OR ULQUALIMIT<55 OR DLQUALIMITAMRFR<55 OR DLQUALIMITAMRHR<55 OR ULQUALIMITAMRFR<55 OR ULQUALIMITAMRHR<55
					THEN "NOK<55"
					ELSE "OK"
			END
				AS CHECK_QualLimits
		FROM 2g_cells_ready G
			LEFT OUTER JOIN batches B ON G.`Site` = B.`Site`
			LEFT OUTER JOIN temp2 T ON G.CELLNAME = T.CELLNAME
			LEFT OUTER JOIN temp_trx r ON G.CELLNAME = r.CELLNAME
			LEFT OUTER JOIN gcellhoemg E ON G.CELLNAME = E.CELLNAME
			LEFT OUTER JOIN gcellamrqul A ON G.CELLNAME = A.CELLNAME
		) AS a
	WHERE ACTSTATUS = 'ACTIVATED' AND (CHECK_BBH_activation = 'NOK' OR CHECK_QualLimits LIKE 'NOK%') ;	
	# Check MAIOs of SFH cells	
DROP TABLE IF EXISTS temp1;
DROP TABLE IF EXISTS gcell_sfh_data;
DROP TABLE IF EXISTS check_2g_sfh_maio;

CREATE TEMPORARY TABLE temp1
	SELECT `BSCName`, LEFT(`CELLNAME`,4) AS Site , CELLNAME, TRXID, `TRXHOPINDEX`,`TRXMAIO`
		FROM gtrxchanhop
		WHERE TRXHOPINDEX <> '255'
		GROUP BY BSCNAME, LEFT(CELLNAME,4), CELLNAME, TRXID, `TRXHOPINDEX`,`TRXMAIO`;
		
CREATE TABLE gcell_sfh_data
	SELECT 	m.`BSCName`, LEFT(m.`CELLNAME`,4) AS Site, m.`CELLNAME`,m.`CELLID`, m.`HOPINDEX`, m.`HSN`, t.TRXID, t.TRXMAIO,
		CASE WHEN m.`FREQ1` BETWEEN 662 AND 735
			THEN '1800'
			ELSE '900'
		END
		AS 'HopBand',
		LEAST(
		CASE WHEN m.`FREQ1` IS NOT NULL
			THEN CAST(m.`FREQ1` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ2` IS NOT NULL
			THEN CAST(m.`FREQ2` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ3` IS NOT NULL
			THEN CAST(m.`FREQ3` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ4` IS NOT NULL
			THEN CAST(m.`FREQ4` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ5` IS NOT NULL
			THEN CAST(m.`FREQ5` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ6` IS NOT NULL
			THEN CAST(m.`FREQ6` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ7` IS NOT NULL
			THEN CAST(m.`FREQ7` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ8` IS NOT NULL
			THEN CAST(m.`FREQ8` AS UNSIGNED)
			ELSE 2000
		END,
		CASE WHEN m.`FREQ9` IS NOT NULL
			THEN CAST(m.`FREQ9` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ10` IS NOT NULL
			THEN CAST(m.`FREQ10` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ11` IS NOT NULL
			THEN CAST(m.`FREQ11` AS UNSIGNED)
			ELSE 2000
		END, 
		CASE WHEN m.`FREQ12` IS NOT NULL
			THEN CAST(m.`FREQ12` AS UNSIGNED)
			ELSE 2000
		END
		) 
		AS Min_SFH_Freq,
	GREATEST(
		CASE WHEN m.`FREQ1` IS NOT NULL
			THEN CAST(m.`FREQ1` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ2` IS NOT NULL
			THEN CAST(m.`FREQ2` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ3` IS NOT NULL
			THEN CAST(m.`FREQ3` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ4` IS NOT NULL
			THEN CAST(m.`FREQ4` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ5` IS NOT NULL
			THEN CAST(m.`FREQ5` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ6` IS NOT NULL
			THEN CAST(m.`FREQ6` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ7` IS NOT NULL
			THEN CAST(m.`FREQ7` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ8` IS NOT NULL
			THEN CAST(m.`FREQ8` AS UNSIGNED)
			ELSE -1
		END,
		CASE WHEN m.`FREQ9` IS NOT NULL
			THEN CAST(m.`FREQ9` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ10` IS NOT NULL
			THEN CAST(m.`FREQ10` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ11` IS NOT NULL
			THEN CAST(m.`FREQ11` AS UNSIGNED)
			ELSE -1
		END, 
		CASE WHEN m.`FREQ12` IS NOT NULL
			THEN CAST(m.`FREQ12` AS UNSIGNED)
			ELSE -1
		END
		) 
		AS Max_SFH_Freq
	FROM `gcellmagrp` m
		LEFT JOIN temp1 t ON m.`CELLNAME` = t.`CELLNAME` AND m.`HOPINDEX` = t.`TRXHOPINDEX`
		JOIN gtrxhop h ON t.`CELLNAME` = h.`CELLNAME` AND t.`TRXID` = h.`TRXID`
	WHERE m.`HOPMODE` = 'RF_FH' AND h.`HOPTYPE` = 'RF_FH'
	ORDER BY Site, CELLNAME, HOPINDEX;

CREATE TABLE check_2g_sfh_maio
	SELECT b.`City Cluster`, a.`BSCName`, a.Site, a.`HOPINDEX`, a.`HopBand`, a.`HSN`, a.`Min_SFH_Freq`, a.`Max_SFH_Freq`, a.`TRXMAIO`, a.MAIO_Count
	FROM (
		SELECT Site, `BSCName`, `HOPINDEX`, `HSN`, `Min_SFH_Freq`, `Max_SFH_Freq`, `HopBand`, `TRXMAIO`, COUNT(TRXMAIO) AS MAIO_Count
		FROM `gcell_sfh_data`
		GROUP BY Site, `BSCName`, `HOPINDEX`, `HSN`, `Min_SFH_Freq`, `Max_SFH_Freq`, `HopBand`, `TRXMAIO`
		)
	AS a
	LEFT JOIN batches b ON b.Site = a.Site
	WHERE a.MAIO_Count > 1;


# Check externals

DROP TABLE IF EXISTS CHECK_GEXT2G;
DROP TABLE IF EXISTS CHECK_UEXT3G;
DROP TABLE IF EXISTS CHECK_UEXT2G;
DROP TABLE IF EXISTS CHECK_LEXTL;
DROP TABLE IF EXISTS CHECK_LEXTU;
DROP TABLE IF EXISTS CHECK_UNFREQPRIO;
DROP TABLE IF EXISTS check_4g_interfreq;
DROP TABLE IF EXISTS temp1;
DROP TABLE IF EXISTS temp2;
DROP TABLE IF EXISTS temp3;
DROP TABLE IF EXISTS temp4;
DROP TABLE IF EXISTS temp5;

CREATE TEMPORARY TABLE temp1
	SELECT BSCName, ci, COUNT(ci) AS EXTDEF_CI_Count
		FROM gext2gcell_txt
		GROUP BY BSCname, ci;
		
CREATE TABLE CHECK_GEXT2G
	SELECT 	B.`City Cluster`, E.`BSCName`, E.`EXT2GCELLID`, E.`EXT2GCELLNAME`, E.`MCC`, E.`MNC`, E.LAC, E.RA, E.CI, E.`BCCH`, E.`NCC`, E.`BCC`, T.EXTDEF_CI_Count,
		C.`BSCName` AS GCELL_BSCName, C.`BTSNAME`, C.`CELLNAME`, C.`BCCH` AS GCELL_BCCH, C.`LAC` AS GCELL_LAC, C.RAC, 
		C.`NCC` AS GCELL_NCC, C.`BCC` AS GCELL_BCC, C.`ACTSTATUS` AS GCELL_ACTSTATUS,
		
		CASE WHEN E.`EXT2GCELLNAME` = C.CELLNAME
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_CELLNAME,
		
		CASE WHEN E.MCC = '284' AND E.MNC='03'
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_MCCMNC,
				
		CASE WHEN E.`LAC` = C.LAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_LAC,
			
		CASE WHEN E.`RA` = C.RAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RAC,	
			
		CASE WHEN E.`BCCH` = C.BCCH
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_BCCH,
		
		CASE WHEN E.`NCC` = C.NCC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_NCC,
		
		CASE WHEN E.`BCC` = C.BCC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_BCC,
		CASE WHEN C.CI IS NULL 
			THEN 'NOT_EXIST'
			ELSE (
				CASE WHEN E.`EXT2GCELLNAME` = C.CELLNAME AND E.MCC ='284' AND E.MNC='03' AND E.`LAC` = C.LAC AND E.`RA` = C.RAC AND E.`BCCH` = C.BCCH 
					AND E.`NCC` = C.NCC AND E.`BCC` = C.BCC
				THEN 'OK'
				ELSE 'NOK'
			END)
		END
		AS CHECK_NOK
		
		FROM gext2gcell_txt E
			LEFT OUTER JOIN cell_2g c ON E.CI = C.CI
			LEFT OUTER JOIN temp1 T ON E.BSCName = T.BSCName AND E.CI = T.CI
			LEFT OUTER JOIN batches B ON LEFT(E.CI, 4) = B.Site
			WHERE C.CI IS NULL OR E.`EXT2GCELLNAME` <> C.CELLNAME OR E.MCC <> '284' OR E.MNC <> '03' OR E.`LAC` <> C.LAC 
					OR E.`RA` <> C.RAC OR E.`BCCH` <> C.BCCH OR E.`NCC` <> C.NCC OR E.`BCC` <> C.BCC;

CREATE TEMPORARY TABLE temp2
	SELECT LOGICRNCID, CELLID, COUNT(CELLID) AS EXTDEF_CI_Count
		FROM unrnccell
		GROUP BY LOGICRNCID, CELLID;
			
CREATE TABLE CHECK_UEXT3G
	SELECT 	B.`City Cluster`, E.`LOGICRNCID`, E.`NRNCID`, E.`CELLID`, E.`CELLNAME`, E.`PSCRAMBCODE`, E.`BANDIND`, E.`UARFCNUPLINK`, E.`UARFCNDOWNLINK`, E.`LAC`, E.`RAC`, 
		E.`CELLCAPCONTAINERFDD`, T.EXTDEF_CI_Count, C.`LOGICRNCID` AS UCELL_RNCID, C.`CELLNAME` AS UCELL_CELLNAME, C.`PSCRAMBCODE` AS UCELL_PSC, 
		C.BANDIND AS UCELL_BANDIND, C.UARFCNDOWNLINK AS UCELL_UARFCNDL, C.LAC AS UCELL_LAC, C.RAC AS UCELL_RAC, C.ACTSTATUS AS UCELL_ACTSTATUS,
		
		CASE WHEN E.`NRNCID` = C.LOGICRNCID
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RNCID,
		
		CASE WHEN E.`CELLNAME` = C.CELLNAME
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_CELLNAME,
						
		CASE WHEN E.`BANDIND` = C.BANDIND
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_BANDIND,
		
		CASE WHEN E.`UARFCNDOWNLINK` = C.UARFCNDOWNLINK
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_UARFCNDL,
				
		CASE WHEN E.`PSCRAMBCODE` = C.PSCRAMBCODE
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_PSC,
				
		CASE WHEN E.`LAC` = C.LAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_LAC,	
		
		CASE WHEN E.`RAC` = C.RAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RAC,	
		CASE WHEN C.CELLID IS NULL
			THEN 'NOT_EXIST'
			ELSE (
				CASE WHEN E.`NRNCID` = C.LOGICRNCID AND E.`CELLNAME` = C.CELLNAME AND E.`BANDIND` = C.BANDIND 
					AND E.`UARFCNDOWNLINK` = C.UARFCNDOWNLINK AND E.`PSCRAMBCODE` = C.PSCRAMBCODE AND E.`LAC` = C.LAC AND E.`RAC` = C.RAC
					THEN 'OK'
					ELSE 'NOK'
				END)
		END
		AS CHECK_NOK	
								
		FROM UNRNCCELL E
			LEFT OUTER JOIN CELL_3G C ON E.NRNCID = C.LOGICRNCID AND E.CELLID = C.CELLID
			LEFT OUTER JOIN temp2 T ON E.LOGICRNCID = T.LOGICRNCID AND E.CELLID = T.CELLID
			LEFT OUTER JOIN batches B ON LEFT(E.CELLNAME, 4) = B.Site
			WHERE C.CELLID IS NULL OR E.`NRNCID` <> C.LOGICRNCID OR E.`CELLNAME` <> C.CELLNAME OR E.`BANDIND` <> C.BANDIND 
					OR E.`UARFCNDOWNLINK` <> C.UARFCNDOWNLINK OR E.`PSCRAMBCODE` <> C.PSCRAMBCODE OR E.`LAC` <> C.LAC OR E.`RAC` <> C.RAC;
/*
CREATE TEMPORARY TABLE temp3
	SELECT LOGICRNCID, CID, COUNT(CID) AS EXTDEF_CI_Count
		FROM ugsmcell
		GROUP BY LOGICRNCID, CID;
				 
CREATE TABLE CHECK_UEXT2G
	SELECT 	B.`City Cluster`, E.LOGICRNCID, E.GSMCELLINDEX, E.GSMCELLNAME, E.MCC, E.MNC, E.LAC, E.RAC, E.CID, E.BCCHARFCN, E.NCC, E.BCC, E.BANDIND, E.RATCELLTYPE, T.EXTDEF_CI_Count,
		C.BSCName AS GCELL_BSCName, C.BTSNAME AS GCELL_BTSName, C.CELLNAME AS GCELL_CELLNAME, C.TYPE AS GCELL_TYPE, C.MCC AS GCELL_MCC, C.MNC AS GCELL_MNC,
		C.LAC AS GCELL_LAC, C.RAC AS GCELL_RAC, C.BCCH AS GCELL_BCCH, C.NCC AS GCELL_NCC, C.BCC AS GCELL_BCC, C.ACTSTATUS AS GCELL_ACTSTATUS,
		
		CASE WHEN E.`GSMCELLNAME` = C.CELLNAME
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_GSMCELLNAME,		
		
		CASE WHEN E.`MCC` = C.MCC AND E.MNC = C.MNC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_MCCMNC,
		
		CASE WHEN E.`LAC` = C.LAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_LAC,				
		
		CASE WHEN E.`RAC` = C.RAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RAC,	
				
		CASE WHEN E.`BCCHARFCN` = C.BCCH
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_BCCH,
				
		CASE WHEN E.`NCC` = C.NCC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_NCC,
				
		CASE WHEN E.`BCC` = C.BCC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_BCC,	
				
		CASE WHEN E.`RATCELLTYPE` = 'EDGE'
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RATCELLTYPE,
		CASE WHEN C.CI IS NULL
			THEN 'NOT_EXIST'
			ELSE (
				CASE WHEN E.`GSMCELLNAME` = C.CELLNAME AND E.`MCC` = C.MCC AND E.MNC = C.MNC AND E.`LAC` = C.LAC AND E.`RAC` = C.RAC 
					AND E.`BCCHARFCN` = C.BCCH AND E.`NCC` = C.NCC AND E.`BCC` = C.BCC AND E.`RATCELLTYPE` = 'EDGE'
					THEN 'OK'
					ELSE 'NOK'
				END)
		END
		AS CHECK_NOK	
						
	FROM UGSMCELL E
		LEFT OUTER JOIN CELL_2G C ON E.CID = C.CI
		LEFT OUTER JOIN temp3 T ON E.LOGICRNCID = T.LOGICRNCID AND E.CID=T.CID
		LEFT OUTER JOIN batches B ON LEFT(E.CID, 4) = B.Site
		WHERE C.CI IS NULL OR E.`GSMCELLNAME` <> C.CELLNAME OR E.`MCC` <> C.MCC OR E.MNC <> C.MNC OR E.`LAC` <> C.LAC OR E.`RAC` <> C.RAC 
				OR E.`BCCHARFCN` <> C.BCCH OR E.`NCC` <> C.NCC OR E.`BCC` <> C.BCC OR E.`RATCELLTYPE` <> 'EDGE';

				
CREATE TABLE CHECK_LEXTL
	SELECT	B.`City Cluster`, E.eNodeB, E.Mcc, E.Mnc, E.eNodeBId, E.CellId, E.DlEarfcn, E.UlEarfcnCfgInd, E.UlEarfcn, E.PhyCellId, E.Tac, E.CellName, E.AnrFlag, 
		C.eNodeBId AS LCELL_eNodeBId, C.Site AS LCELL_Site, C.CellId AS LCELL_CellId, C.CellName AS LCELL_CellName, C.PCI AS LCELL_PCI, C.TAC AS LCELL_TAC, C.DlEarfcn AS LCELL_DlEarfcn, 
		C.CellActiveState AS LCELL_ACTSTATE,
		
		CASE WHEN E.`MCC` = '284' AND E.MNC = '03' 
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_MCCMNC, 
		
		CASE WHEN E.`DlEarfcn` = C.DlEarfcn
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_DlEarfcn, 		
		
		CASE WHEN E.`PhyCellId` = C.PCI
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_PCI, 
				
		CASE WHEN E.`Tac` = C.TAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_TAC, 
		
				
		CASE WHEN E.`CellName` = C.CellName
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_CellName,
		
		
		CASE WHEN C.CellId IS NULL 
			THEN 'NOT_EXIST'
			ELSE (
				CASE WHEN E.`MCC` = '284' AND E.MNC = '03' AND E.`DlEarfcn` = C.DlEarfcn AND E.`PhyCellId` = C.PCI AND E.`Tac` = C.TAC
					THEN 'OK'
					ELSE 'NOK'
				END)
		END
		AS CHECK_NOK		
					
	FROM leutranexternalcell E
		LEFT OUTER JOIN CELL_4G C ON E.eNodeBId = C.Site AND E.CellId = C.CellId
		LEFT OUTER JOIN batches B ON E.eNodeBId = B.Site
		WHERE C.CellId IS NULL OR E.`MCC` <> '284' OR E.MNC <> '03' OR E.`DlEarfcn` <> C.DlEarfcn OR E.`PhyCellId` <> C.PCI OR E.`Tac` <> C.TAC;
	
CREATE TABLE CHECK_LEXTU
	SELECT 	B.`City Cluster`, E.eNodeB, E.RncId, E.CellId, E.CellName, E.UtranDlArfcn, E.UtranUlArfcn, E.UtranFddTddType, E.Rac, E.PScrambCode, E.Lac, E.CsPsHoInd, E.AnrFlag,
		E.CtrlMode, C.LOGICRNCID AS UCELL_RNCId, C.NODEBNAME AS UCELL_NODEBNAME, C.CELLNAME AS UCELL_CELLNAME, C.UARFCNDOWNLINK AS UCELL_ARFCNDL, 
		C.PSCRAMBCODE AS UCELL_PSC, C.LAC AS UCELL_LAC, C.RAC AS UCELL_RAC, C.ACTSTATUS AS UCELL_ACTSTATUS, 
						
		CASE WHEN E.`RncId` = C.LOGICRNCID
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RNCId, 
		
					
		CASE WHEN E.`CellName` = C.CELLNAME
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_CellName, 
		
						
		CASE WHEN E.`UtranDlArfcn` = C.UARFCNDOWNLINK
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_UARFCNDL, 
						
		CASE WHEN E.`PScrambCode` = C.PSCRAMBCODE
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_PSC, 
						
		CASE WHEN E.`Lac` = C.LAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_LAC, 
								
		CASE WHEN E.`Rac` = C.RAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_RAC, 
								
		CASE WHEN E.`RncId` = C.LOGICRNCID  AND E.`CellName` = C.CELLNAME  AND E.`UtranDlArfcn` = C.UARFCNDOWNLINK AND E.`PScrambCode` = C.PSCRAMBCODE AND E.`Lac` = C.LAC AND E.`Rac` = C.RAC
			THEN 'OK'
			ELSE 'NOK'
		END
		AS CHECK_NOK
		 
		FROM lutranexternalcell E
			LEFT OUTER JOIN cell_3g C ON E.CellId = C. CELLID
			LEFT OUTER JOIN batches B ON SUBSTRING(E.eNodeB,3,4) = B.Site
			WHERE E.`RncId` <> C.LOGICRNCID OR E.`UtranDlArfcn` <> C.UARFCNDOWNLINK OR E.`PScrambCode` <> C.PSCRAMBCODE OR E.`Lac` <> C.LAC OR E.`Rac` <> C.RAC;

*/
# Check for wrong configurations of EARFCNs in 3G UCELLNFREQPRIOINFO
CREATE TEMPORARY TABLE temp4
	SELECT Site, Sector_ID, Dlearfcn, ROW_NUMBER() OVER (PARTITION BY CONCAT(Site,Sector_ID) ORDER BY CAST(Dlearfcn AS UNSIGNED)) AS Ranked
	FROM Cell_4G
	WHERE `CellActiveState` = 'CELL_ACTIVE';
	
CREATE TEMPORARY TABLE temp5 (INDEX Site (`Site`), INDEX Sector_ID (`Sector_ID`))
	SELECT Site, Sector_ID, CONCAT_WS('&','',
		MAX(CASE WHEN Ranked = 1 THEN Dlearfcn
		END),
		MAX(CASE WHEN Ranked = 2 THEN Dlearfcn
		END),
		MAX(CASE WHEN Ranked = 3 THEN Dlearfcn
		END),
		MAX(CASE WHEN Ranked = 4 THEN Dlearfcn
		END),'')
		AS Existing_LTE_Layers
		FROM temp4
		GROUP BY Site, Sector_ID ORDER BY Site;
			
CREATE TABLE CHECK_UNFREQPRIO
	SELECT DISTINCT `City Cluster`, U.LOGICRNCID, U.NODEBNAME, U.CELLNAME, U.CELLID, U.ACTSTATUS, 
			N.EARFCN, N.NPRIORITY, N.EMEASBW, N.EQRXLEVMIN, N.EDETECTIND, R.SPRIORITY, N.FREQUSEPOLICYIND,	
			CASE WHEN Existing_LTE_Layers IS NOT NULL
				THEN CONCAT('Co-Cell LTE Layer ',Existing_LTE_Layers,' exist')
				ELSE 'Co-Cell LTE Layer NOT exist'
			END
			AS 'Co-LTE',
			CASE WHEN N.NPRIORITY>R.SPRIORITY
				THEN 'LTE'
				ELSE 'UMTS'
			END
			AS CHECK_Priority_to,
			'Configured_EARFCN' AS CHECK_nfreq
		FROM CELL_3G U
			LEFT OUTER JOIN ucellnfreqprioinfo N ON U.CELLNAME = N.CELLNAME
			LEFT OUTER JOIN ucellselresel R ON U.CELLNAME = R.CELLNAME
			LEFT OUTER JOIN temp5 L ON U.Site = L.Site AND U.Sector_ID = L.Sector_ID
			LEFT OUTER JOIN batches B ON U.Site = B.Site
		WHERE 	(
			(N.NPRIORITY<=R.SPRIORITY AND LOCATE(N.EARFCN,Existing_LTE_Layers)>0) OR
			(N.NPRIORITY>R.SPRIORITY AND Existing_LTE_Layers IS NULL)
			) AND 
			U.ACTSTATUS = 'ACTIVATED' ;
			
 # Check for missing EARFCN according to UCELLNFREQPRIOINFO and existing LTE co-cells		
INSERT INTO CHECK_UNFREQPRIO (`City Cluster`, LOGICRNCID, NODEBNAME, CELLNAME, CELLID, ACTSTATUS, EARFCN, `Co-LTE`, CHECK_Priority_to, CHECK_nfreq)
	SELECT `City Cluster`, U.LOGICRNCID, U.NODEBNAME, U.CELLNAME, U.CELLID, U.ACTSTATUS, L.Dlearfcn, 
		CONCAT('Co-Cell LTE Layer ',Existing_LTE_Layers,' exist'), 'N/A', 'Missing_EARFCN'
		FROM CELL_3G U
			LEFT OUTER JOIN temp4 L ON U.Site = L.Site AND U.Sector_ID = L.Sector_ID
			LEFT OUTER JOIN temp5 M ON U.Site = M.Site AND U.Sector_ID = M.Sector_ID
			LEFT OUTER JOIN ucellnfreqprioinfo N ON LEFT(N.`CELLNAME`, 4) = L.Site AND MID(N.`CELLNAME`,5,1) = L.Sector_ID AND N.`EARFCN` = L.Dlearfcn
			LEFT OUTER JOIN batches B ON U.Site = B.Site
		WHERE	L.Dlearfcn IS NOT NULL AND 
			N.`EARFCN` IS NULL AND
			U.ACTSTATUS = 'ACTIVATED' ;			
/*
CREATE TABLE check_4g_interfreq
	SELECT 	`City Cluster`, `eNodeB`, Site, `LocalCellId`, `CellName`, `Sector_ID`, Carrier_ID, CellResel_ReselPriority,
		CellResel_SNonIntraSearch, CellResel_ThrshServLow, `DlEarfcn`, `CellReselPriorityCfgInd`, `CellReselPriority`, `MeasBandWidth`,
		`QoffsetFreq`, `ThreshXhigh`,`ThreshXlow`, `QRxLevMin`, Existing_LTE_Layers, Priority_to, 
		CHECK_EutranReselTime, CHECK_QRxLevMin, CHECK_Pmax, CHECK_Interfreq_ReselPriority, CHECK_Interfreq_Priority_Threshold,
		CHECK_Interfreq_CellResel_Params
	FROM (	
			SELECT 	b.`City Cluster`, c.`eNodeB`, c.Site, c.`LocalCellId`, c.`CellName`, c.`Sector_ID`, c.Carrier_ID, 
				s.`CellReselPriority` AS CellResel_ReselPriority, s.`SNonIntraSearch` AS CellResel_SNonIntraSearch, s.`ThrshServLow` AS CellResel_ThrshServLow,
				l.`DlEarfcn`, l.`CellReselPriorityCfgInd`, l.`CellReselPriority`, l.`MeasBandWidth`, l.`QoffsetFreq`, l.`ThreshXhigh`,l.`ThreshXlow`,l.`QRxLevMin`, 
				e.Existing_LTE_Layers, 
				CASE 	WHEN l.`CellReselPriority` IS NOT NULL AND s.`CellReselPriority` IS NOT NULL
					THEN 	CASE WHEN s.`CellReselPriority` > l.`CellReselPriority` 
							THEN 'Serving_Layer'
							ELSE 	CASE WHEN s.`CellReselPriority` < l.`CellReselPriority` 
									THEN 'Nbr_Layer'
									ELSE 'Equal_Priority'
								END
						END
					WHEN l.`CellReselPriority` IS NOT NULL AND s.`CellReselPriority` IS NULL THEN 'ServingPriority_NotConfigured'
					WHEN l.`CellReselPriority` IS NULL AND s.`CellReselPriority` IS NOT NULL THEN 'NbrPriority_NotConfigured'
					ELSE 'N/A'
				END
				AS 'Priority_to',
				CASE WHEN l.`EutranReselTime` <> '1' 
					THEN 'NOK<>1'
					ELSE 'OK'
				END AS 'CHECK_EutranReselTime',
				CASE WHEN l.`QRxLevMin` <> '-64' 
					THEN 'NOK<>-64'
					ELSE 'OK'
				END AS 'CHECK_QRxLevMin',
				CASE WHEN l.`Pmax`<>''
					THEN 'NOK<>NULL'
					ELSE 'OK'
				END AS 'CHECK_Pmax',
				CASE WHEN LOCATE(CONCAT('&',l.`DlEarfcn`,'&'), Existing_LTE_Layers) <> 0
					THEN 	CASE 	WHEN l.`CellReselPriorityCfgInd` <> 'NOT_CFG'
							THEN CASE 
								WHEN c.Carrier_ID = 'LTE_900' 
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' 							AND l.`CellReselPriority` <> '7' THEN 'NOK_L9->L26_Priority<>7'
											WHEN l.`DlEarfcn` = '3150' 							AND l.`CellReselPriority` = '7' THEN 'OK'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`CellReselPriority` <> '6' THEN 'NOK_L9->L18_Priority<>6'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`CellReselPriority` = '6' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` <> '6' THEN 'NOK_L9->L21_Priority<>6'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` = '6' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_1800'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' 							AND l.`CellReselPriority` <> '7' THEN 'NOK_L18->L26_Priority<>7'
											WHEN l.`DlEarfcn` = '3150' 							AND l.`CellReselPriority` = '7' THEN 'OK'
											WHEN l.`DlEarfcn` = '3516' 							AND l.`CellReselPriority` <> '4' THEN 'NOK_L18->L9_Priority<>4'
											WHEN l.`DlEarfcn` = '3516' 							AND l.`CellReselPriority` = '4' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` <> '6' THEN 'NOK_L18->L21_Priority<>6'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` = '6' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2100'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' AND l.`CellReselPriority` <> '7' THEN 'NOK_L21->L26_Priority<>7'
											WHEN l.`DlEarfcn` = '3150' AND l.`CellReselPriority` = '7' THEN 'OK'
											WHEN l.`DlEarfcn` = '3516' AND l.`CellReselPriority` <> '4' THEN 'NOK_L21->L9_Priority<>4'
											WHEN l.`DlEarfcn` = '3516' AND l.`CellReselPriority` = '4' THEN 'OK'
											WHEN l.`DlEarfcn` = '1575' AND l.`CellReselPriority` <> '6' THEN 'NOK_L21->L18_Priority<>6'
											WHEN l.`DlEarfcn` = '1575' AND l.`CellReselPriority` = '6' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2600'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3516' 							AND l.`CellReselPriority` <> '4' THEN 'NOK_L26->L9_Priority<>4'
											WHEN l.`DlEarfcn` = '3516' 							AND l.`CellReselPriority` = '4' THEN 'OK'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`CellReselPriority` <> '6' THEN 'NOK_L26->L18_Priority<>6'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`CellReselPriority` = '6' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` <> '6' THEN 'NOK_L26->L21_Priority<>6'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`CellReselPriority` = '6' THEN 'OK'
											ELSE 'N/A'
										END
								ELSE 'N/A'
								END
							ELSE 'NOK_Missing_PriorityConfig'
						END
					ELSE 	CASE 	WHEN l.`DlEarfcn` IS NULL 
							THEN 'N/A'
							ELSE 'Configured_EARFCN_NotExist'
						END
				END
				AS 'CHECK_Interfreq_ReselPriority',
				CASE WHEN LOCATE(CONCAT('&',l.`DlEarfcn`,'&'), Existing_LTE_Layers) <> 0
							THEN CASE 
								WHEN c.Carrier_ID = 'LTE_900' 
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' 							AND l.`ThreshXhigh` <> '7' THEN 'NOK_L9->L26_ThreshXhigh<>7'
											WHEN l.`DlEarfcn` = '3150' 							AND l.`ThreshXhigh` = '7' THEN 'OK'							
											WHEN l.`DlEarfcn` = '1575' 							AND l.`ThreshXhigh` <> '7' THEN 'NOK_L9->L18_ThreshXhigh<>7'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`ThreshXhigh` = '7' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXhigh` <> '7' THEN 'NOK_L9->L21_ThreshXhigh<>7'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXhigh` = '7' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_1800'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' 							AND l.`ThreshXhigh` <> '7' THEN 'NOK_L18->L26_ThreshXhigh<>7'
											WHEN l.`DlEarfcn` = '3150' 							AND l.`ThreshXhigh` = '7' THEN 'OK'				
											WHEN l.`DlEarfcn` = '3516' 							AND l.`ThreshXlow` <> '7' THEN 'NOK_L18->L9_ThreshXlow<>7'
											WHEN l.`DlEarfcn` = '3516' 							AND l.`ThreshXlow` = '7' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXlow` <> '7' THEN 'NOK_L18->L21_ThreshXlow<>7'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXlow` = '7' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2100'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' AND l.`ThreshXhigh` <> '7' THEN 'NOK_L21->L26_ThreshXhigh<>7'
											WHEN l.`DlEarfcn` = '3150' AND l.`ThreshXhigh` = '7' THEN 'OK'				
											WHEN l.`DlEarfcn` = '1575' AND l.`ThreshXhigh` <> '7' THEN 'NOK_L21->L18_ThreshXhigh<>7'
											WHEN l.`DlEarfcn` = '1575' AND l.`ThreshXhigh` = '7' THEN 'OK'
											WHEN l.`DlEarfcn` = '3516' AND l.`ThreshXlow` <> '7' THEN 'NOK_L21->L9_ThreshXlow<>7'
											WHEN l.`DlEarfcn` = '3516' AND l.`ThreshXlow` = '7' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2600'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '1575'							AND l.`ThreshXlow` <> '7' THEN 'NOK_L26->L18_ThreshXlow<>7'
											WHEN l.`DlEarfcn` = '1575' 							AND l.`ThreshXlow` = '7' THEN 'OK'
											WHEN l.`DlEarfcn` = '3516'							AND l.`ThreshXlow` <> '7' THEN 'NOK_L26->L9_ThreshXlow<>7'
											WHEN l.`DlEarfcn` = '3516' 							AND l.`ThreshXlow` = '7' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXlow` <> '7' THEN 'NOK_L26->L21_ThreshXlow<>7'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND l.`ThreshXlow` = '7' THEN 'OK'
											ELSE 'N/A'
										END
								ELSE 'N/A'
								END
					ELSE 	CASE 	WHEN l.`DlEarfcn` IS NULL 
							THEN 'N/A'
							ELSE 'Configured_EARFCN_NotExist'
						END
				END
				AS 'CHECK_Interfreq_Priority_Threshold',
				CASE WHEN LOCATE(CONCAT('&',l.`DlEarfcn`,'&'), Existing_LTE_Layers) <> 0
							THEN CASE
								WHEN c.Carrier_ID = 'LTE_900'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L26_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L9->L26_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L26_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L18_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L9->L18_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L18_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'				
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L21_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L9->L21_SNonIntraSearch<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L9->L21_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_1800'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L9_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L18->L9_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L9_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L26_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L18->L26_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L26_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'				
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L21_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L18->L21_SNonIntraSearch<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L18->L21_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2100'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3516' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L9_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L21->L9_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3516' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L9_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											WHEN l.`DlEarfcn` = '3150' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L26_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L21->L26_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3150' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L26_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3150' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'									
											WHEN l.`DlEarfcn` = '1575' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L18_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L21->L18_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '1575' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L21->L18_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											ELSE 'N/A'
										END
								WHEN c.Carrier_ID = 'LTE_2600'
									THEN 	CASE 
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L9_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L26->L9_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L9_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '3516' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L18_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L26->L18_SNonIntraSearch<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L18_ThrshServLow<>5'
											WHEN l.`DlEarfcn` = '1575' 							AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525') 	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L21_SNonIntraSearch<>5_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` <> '5' AND s.`ThrshServLow` ='5' THEN 'NOK_L26->L21_SNonIntraSearch<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` <>'5' THEN 'NOK_L26->L21_ThrshServLow<>5'
											WHEN (l.`DlEarfcn` = '575' OR l.`DlEarfcn` = '550' OR l.`DlEarfcn` = '525')  	AND s.`SNonIntraSearch` = '5' AND s.`ThrshServLow` ='5' THEN 'OK'
											ELSE 'N/A'
										END
								ELSE 'N/A'
								END
					ELSE 	CASE 	WHEN l.`DlEarfcn` IS NULL 
							THEN 'N/A'
							ELSE 'Configured_EARFCN_NotExist'
						END
				END
				AS 'CHECK_Interfreq_CellResel_Params'
			FROM cell_4g c 
				LEFT JOIN `leutraninternfreq` l ON c.`eNodeB` = l.`eNodeB` AND c.`LocalCellId` = l.`LocalCellId`
				LEFT JOIN `lcellresel` s ON c.`eNodeB` = s.`eNodeB` AND c.`LocalCellId` = s.`LocalCellId`
				LEFT JOIN temp5 e ON c.`Site` = e.Site AND c.`Sector_ID` = e.Sector_ID
				LEFT JOIN batches b ON c.Site = b.Site
		) AS a
	WHERE 	CHECK_EutranReselTime 			<> 'OK' OR  
		CHECK_QRxLevMin 			<> 'OK' OR  
		CHECK_Pmax 				<> 'OK' OR 
		CHECK_Interfreq_ReselPriority 		<> 'OK' OR
		CHECK_Interfreq_Priority_Threshold 	<> 'OK' OR
		CHECK_Interfreq_CellResel_Params 	<> 'OK';		
*/
			
# Check neighbours

DROP TABLE IF EXISTS CHECK_COUNT_G2GNCELL;
DROP TABLE IF EXISTS CHECK_COUNT_UINTRAFREQNCELL;
DROP TABLE IF EXISTS CHECK_COUNT_UINTERFREQNCELL;
DROP TABLE IF EXISTS CHECK_COUNT_U2GNCELL;
DROP TABLE IF EXISTS CHECK_COUNT_L2LINTRANCELL;
DROP TABLE IF EXISTS CHECK_COUNT_L2LINTERNCELL;
DROP TABLE IF EXISTS CHECK_COUNT_L2UNCELL;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_G2G;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_U2UINTRA;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_U2UINTER;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_U2G;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_L2LINTRA;
DROP TABLE IF EXISTS CHECK_MISSING_COSITE_L2LINTER;
DROP TABLE IF EXISTS CHECK_MISSING_CoCell_L2U;
DROP TABLE IF EXISTS CHECK_oneway_nbrs_g2g;
DROP TABLE IF EXISTS CHECK_oneway_nbrs_u2uintra;


# Check neighbours count
CREATE TABLE CHECK_COUNT_G2GNCELL (INDEX `CI` (`CI`), INDEX `CELLNAME` (`CELLNAME`))
	SELECT `City Cluster`, C.`BTSNAME`, C.Site, C.BSCName, C.`CELLNAME`, C.`CI`, C.`ACTSTATUS`, COUNT(N.SRCCELLNAME) AS G2GNCELL_Count
	FROM cell_2g C
		LEFT OUTER JOIN g2gncell N ON C.`CELLNAME` = N.`SRCCELLNAME`
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`ACTSTATUS` = 'ACTIVATED'
	GROUP BY CELLNAME ORDER BY G2GNCELL_Count;

CREATE TABLE CHECK_COUNT_UINTRAFREQNCELL (INDEX `CELLID` (`CELLID`))
	SELECT `City Cluster`, C.`NODEBNAME`, C.Site, C.LOGICRNCID, C.`CELLNAME`, C.`CELLID`, C.`ACTSTATUS`, COUNT(N.CELLNAME) AS UINTRAFREQNCELL_Count
	FROM cell_3g C
		LEFT OUTER JOIN uintrafreqncell N ON C.`CELLNAME` = N.`CELLNAME`
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`ACTSTATUS` = 'ACTIVATED'		
	GROUP BY CELLNAME ORDER BY UINTRAFREQNCELL_Count;
					
CREATE TABLE CHECK_COUNT_UINTERFREQNCELL
	SELECT `City Cluster`, C.`NODEBNAME`, C.Site, C.LOGICRNCID, C.`CELLNAME`, C.`CELLID`, C.`ACTSTATUS`, COUNT(N.CELLNAME) AS UINTERFREQNCELL_Count
	FROM cell_3g C
		LEFT OUTER JOIN uinterfreqncell N ON C.`CELLNAME` = N.`CELLNAME`
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`ACTSTATUS` = 'ACTIVATED'	
	GROUP BY CELLNAME ORDER BY UINTERFREQNCELL_Count;
	
/*					
CREATE TABLE CHECK_COUNT_U2GNCELL
	SELECT `City Cluster`, C.`NODEBNAME`, C.Site, C.LOGICRNCID, C.`CELLNAME`, C.`CELLID`, C.`ACTSTATUS`, COUNT(N.CELLNAME) AS U2GNCELL_Count
	FROM cell_3g C
		LEFT OUTER JOIN ugsmncell N ON C.`CELLNAME` = N.`CELLNAME`
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`ACTSTATUS` = 'ACTIVATED'	
	GROUP BY CELLNAME ORDER BY U2GNCELL_Count;
					
CREATE TABLE CHECK_COUNT_L2LINTRANCELL
	SELECT `City Cluster`, C.Site, C.`eNodeB`,  C.`CELLID`, C.`CELLNAME`, C.`CellActiveState`, COUNT(CONCAT(N.eNodeB, N.LocalCellId)) AS L2LINTRANCELL_Count
	FROM cell_4g C
		LEFT OUTER JOIN leutranintrafreqncell N ON C.`eNodeB` = N.`eNodeB` AND C.LocalCellId = N.LocalCellId
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`CellActiveState` = 'CELL_ACTIVE'
	GROUP BY CELLNAME ORDER BY L2LINTRANCELL_Count;	
					
CREATE TABLE CHECK_COUNT_L2LINTERNCELL
	SELECT `City Cluster`, C.Site, C.`eNodeB`, C.`CELLID`, C.`CELLNAME`, C.`CellActiveState`, COUNT(CONCAT(N.eNodeB, N.LocalCellId)) AS L2LINTERNCELL_Count
	FROM cell_4g C
		LEFT OUTER JOIN leutraninterfreqncell N ON C.`eNodeB` = N.`eNodeB` AND C.LocalCellId = N.LocalCellId
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`CellActiveState` = 'CELL_ACTIVE'		
	GROUP BY CELLNAME ORDER BY L2LINTERNCELL_Count;	

CREATE TABLE CHECK_COUNT_L2UNCELL
	SELECT `City Cluster`, C.Site, C.`eNodeB`, C.`CELLID`, C.`CELLNAME`, C.`CellActiveState`, COUNT(CONCAT(N.eNodeB, N.LocalCellId)) AS L2UNCELL_Count
	FROM cell_4g C
		LEFT OUTER JOIN lutranncell N ON C.`eNodeB` = N.`eNodeB` AND C.LocalCellId = N.LocalCellId
		LEFT OUTER JOIN batches B ON C.`Site` = B.`Site`
	WHERE C.`CellActiveState` = 'CELL_ACTIVE'
	GROUP BY CELLNAME ORDER BY L2UNCELL_Count;

*/

# Check missing co-site neighbours		
CREATE TABLE CHECK_MISSING_COSITE_G2G
	SELECT 	`City Cluster`, S.BSCName, S.BTSName, S.Site, S.`CELLNAME`, S.CI, S.ACTSTATUS, N.`CELLNAME` AS NBRCELLNAME, N.CI AS NBRCI, N.ACTSTATUS AS NBR_ACTSTATUS,
		CONCAT(S.CI, '_', N.CI) AS SRCCI_NBRCI
	FROM CELL_2G S
		INNER JOIN CELL_2G N ON S.Site = N.Site
		LEFT OUTER JOIN nbrs_g2g T ON CONCAT(S.CI, '_', N.CI) = T.`SRCCI_NBRCI`
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	T.`SRCCI_NBRCI` IS NULL AND 
		S.CI <> N.CI AND
		S.ACTSTATUS = 'ACTIVATED' AND
		N.ACTSTATUS = 'ACTIVATED';

CREATE TABLE CHECK_MISSING_COSITE_U2UINTRA			
	SELECT  `City Cluster`, S.LOGICRNCID, S.NODEBNAME, S.Site, S.`CELLNAME`, S.CELLID, S.ACTSTATUS, N.`CELLNAME` AS NBRCELLNAME, N.CELLID AS NBRCELLID, 
		N.ACTSTATUS AS NBR_ACTSTATUS, CONCAT(S.CELLID, '_', N.CELLID) AS SRCCI_NBRCI
	FROM CELL_3G S
		INNER JOIN CELL_3G N ON S.Site = N.Site AND S.Layer = N.Layer
		LEFT OUTER JOIN nbrs_u2uintra T ON CONCAT(S.CELLID, '_', N.CELLID) = T.`SRCCI_NBRCI`
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	T.`SRCCI_NBRCI` IS NULL AND 
		S.CELLID <> N.CELLID AND 
		S.ACTSTATUS = 'ACTIVATED' AND
		N.ACTSTATUS = 'ACTIVATED';
			
CREATE TABLE CHECK_MISSING_COSITE_U2UINTER			
	SELECT  `City Cluster`, LOGICRNCID, NODEBNAME, Site, Sector_ID, CoSector_Layers, `CELLNAME`, CELLID, ACTSTATUS, NBRCELLNAME, NBRCELLID, 
		NBR_ACTSTATUS, IDLEQOFFSET2SN, HOCOVPRIO, BLINDHOFLAG, BLINDHOQUALITYCONDITION, SRCCI_NBRCI, M2000_SRCCI_NBRCI, RelationType, 
		CHECK_INTERFREQ_CoCell, CHECK_IdleQoffset2sn, CHECK_HOCOVPRIO, CHECK_BLINDHOFLAG
	FROM (
		SELECT  `City Cluster`, S.LOGICRNCID, S.NODEBNAME, S.Site, S.Sector_ID, c.CoSector_Layers, S.`CELLNAME`, S.CELLID, S.ACTSTATUS, N.`CELLNAME` AS NBRCELLNAME, N.CELLID AS NBRCELLID, 
			N.ACTSTATUS AS NBR_ACTSTATUS, T.IDLEQOFFSET2SN, T.HOCOVPRIO, BLINDHOFLAG, BLINDHOQUALITYCONDITION, CONCAT(S.CELLID, '_', N.CELLID) AS SRCCI_NBRCI, T.SRCCI_NBRCI AS M2000_SRCCI_NBRCI,
			CASE WHEN LOCATE('U9',c.CoSector_Layers) <>0
				THEN 	(CASE 	WHEN S.Layer = 3 AND N.Layer <> 6 THEN 'U9->F1/F2'
						WHEN S.Layer = 3 AND N.Layer = 6 THEN 'U9->F3'
						WHEN S.Layer = 4 AND N.Layer <> 6 THEN 'F1->F2/U9'
						WHEN S.Layer = 4 AND N.Layer = 6 THEN 'F1->F3'
						WHEN S.Layer = 5 AND N.Layer <> 4 THEN 'F2->F3/U9'
						WHEN S.Layer = 5 AND N.Layer = 4 THEN 'F2->F1'
						WHEN S.Layer = 6 AND N.Layer <> 5 THEN 'F3->F1/U9'
						WHEN S.Layer = 6 AND N.Layer = 5 THEN 'F3->F2'
						ELSE 'N/A'
					END)
				ELSE 	(CASE 	WHEN S.Layer = 4 AND N.Layer = 5 THEN 'F1->F2'
						WHEN S.Layer = 4 AND N.Layer = 6 THEN 'F1->F3'
						WHEN S.Layer = 5 AND N.Layer = 4 THEN 'F2->F1'
						WHEN S.Layer = 5 AND N.Layer = 6 THEN 'F2->F3'
						WHEN S.Layer = 6 AND N.Layer = 4 THEN 'F3->F1'
						WHEN S.Layer = 6 AND N.Layer = 5 THEN 'F3->F2'
						ELSE 'N/A'
				
					END)
				END
			AS RelationType,
			 CASE WHEN T.`SRCCI_NBRCI` IS NULL 
				THEN	(CASE WHEN LOCATE('U9',c.CoSector_Layers) <>0 
						THEN 	(CASE WHEN LOCATE('F3',c.CoSector_Layers) <>0
								THEN	(CASE WHEN (S.Layer = 3 AND N.Layer <> 6) OR (S.Layer = 4 AND N.Layer <> 6) OR (S.Layer = 5 AND N.Layer <> 4) OR (S.Layer = 6 AND N.Layer <> 5) 
										THEN 'Missing'
										ELSE 'Forbidden'
									END)
								ELSE 'Missing'
							END)
						ELSE 'Missing'
					END)
				ELSE 	(CASE WHEN LOCATE('U9',c.CoSector_Layers) <>0 
						THEN 	(CASE WHEN LOCATE('F3',c.CoSector_Layers) <>0
								THEN	(CASE WHEN (S.Layer = 3 AND N.Layer = 6) OR (S.Layer = 4 AND N.Layer = 6) OR (S.Layer = 5 AND N.Layer = 4) OR (S.Layer = 6 AND N.Layer = 5) 
										THEN 'Forbidden'
										ELSE 'OK'
									END)
								ELSE 'OK'
							END)
						ELSE 'OK'
					END)
			END
			AS CHECK_INTERFREQ_CoCell,
			CASE WHEN S.Layer = 3 AND N.Layer <> 6 
				THEN 	(CASE WHEN IDLEQOFFSET2SN = '-3'
						THEN 'OK'
						ELSE 'NOK'
					END)
				ELSE 	(CASE WHEN IDLEQOFFSET2SN = '0'
						THEN 'OK'
						ELSE 'NOK'
					END)
				
			END
			AS CHECK_IdleQoffset2sn,
			
			CASE WHEN S.Layer <> 3 AND N.Layer <> 3 
				THEN (	CASE WHEN HOCOVPRIO = '0'
						THEN 'OK'
						ELSE 'NOK'
					END)
				ELSE (	CASE WHEN HOCOVPRIO = '2'
						THEN 'OK'
						ELSE 'NOK'
					END)
			END
			AS CHECK_HOCOVPRIO,
			
			CASE WHEN LOCATE('U9',c.CoSector_Layers) <>0 AND LOCATE('F3',c.CoSector_Layers) <>0 
				THEN	(CASE WHEN S.Layer <> 3 AND N.Layer <> 3 
						THEN 	(CASE WHEN BLINDHOFLAG = 'TRUE'
								THEN 'OK'
								ELSE 'NOK'
							END)
						ELSE 	(CASE WHEN BLINDHOFLAG = 'FALSE'
								THEN 'OK'
								ELSE 'NOK'
							END)
					END)
				ELSE 	(CASE WHEN BLINDHOFLAG = 'FALSE'
						THEN 'OK'
						ELSE 'NOK'
					END)
			END
			AS CHECK_BLINDHOFLAG
				
		FROM CELL_3G S
			INNER JOIN CELL_3G N ON S.Site = N.Site AND S.Sector_ID = N.Sector_ID
			LEFT OUTER JOIN nbrs_u2uinter T ON CONCAT(S.CELLID, '_', N.CELLID) = T.`SRCCI_NBRCI`
			LEFT OUTER JOIN batches B ON S.Site = B.Site
			LEFT OUTER JOIN cosector_layers c ON S.Site = c.Site AND S.Sector_ID = c.Sector_ID
		WHERE 	S.CELLID <> N.CELLID AND
			S.ACTSTATUS = 'ACTIVATED' AND
			N.ACTSTATUS = 'ACTIVATED'
		) AS a
	WHERE 	CHECK_INTERFREQ_CoCell 	<> 'OK' OR 
		CHECK_IdleQoffset2sn 	<> 'OK' OR
		CHECK_HOCOVPRIO 	<> 'OK';	

/*
CREATE TABLE CHECK_MISSING_COSITE_U2G			
	SELECT  `City Cluster`, S.LOGICRNCID, S.NODEBNAME, S.Site, S.`CELLNAME`, S.CELLID, S.ACTSTATUS, N.`CELLNAME` AS NBRCELLNAME, N.CI AS NBRCI, 
		N.ACTSTATUS AS NBR_ACTSTATUS, CONCAT(S.CELLID, '_', N.CI) AS SRCCI_NBRCI,
		CASE WHEN S.Sector_ID = N.Sector_ID
			THEN 'Co-Sector'
			ELSE 'Co-Site'
		END
		AS RelationType
	FROM CELL_3G S
		LEFT OUTER JOIN CELL_2G N ON S.Site = N.Site
		LEFT OUTER JOIN nbrs_u2g T ON CONCAT(S.CELLID, '_', N.CI) = T.`SRCCI_NBRCI`
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	T.`SRCCI_NBRCI` IS NULL AND 
		N.CI IS NOT NULL AND 
		S.ACTSTATUS = 'ACTIVATED' AND
		N.ACTSTATUS = 'ACTIVATED';	
			
CREATE TABLE CHECK_MISSING_CoCell_L2U		
	SELECT	`City Cluster`, eNodeB, ENodeBId, CellName, CellId, `CellActiveState`, NBR_RNCId, NBRCELLID, NBRCELLNAME, NODEBNAME, NBR_ACTSTATUS, 
		AnrFlag, BlindHoPriority, SRCNAME_NBRNAME, U2020_SRCNAME_NBRNAME, Relation, CoCell_RelationType, Missing_L2U_CoCell, 
		CHECK_BlindHoPriority
	FROM (
		SELECT	`City Cluster`, S.eNodeB, S.ENodeBId, S.CellName, S.CellId, S.`CellActiveState`, N.LOGICRNCID AS NBR_RNCId, N.CELLID AS NBRCELLID, 
			N.CELLNAME AS NBRCELLNAME, N.NODEBNAME, N.ACTSTATUS AS NBR_ACTSTATUS, T.AnrFlag, T.BlindHoPriority, 
			CONCAT(S.CellName,'_', N.CELLNAME) AS SRCNAME_NBRNAME, T.SRCNAME_NBRNAME AS U2020_SRCNAME_NBRNAME,
			CASE WHEN S.Sector_ID = N.Sector_ID
				THEN 'Co-Sector'
				ELSE 'Co-Site'
			END
			AS Relation,
			CASE WHEN S.Sector_ID = N.Sector_ID
				THEN (	CASE 	WHEN S.Layer = 7 AND N.Layer <> 3 THEN 'L900->U2100'
						WHEN S.Layer = 7 AND N.Layer = 3 THEN 'L900->U900'
						WHEN S.Layer = 8 AND N.Layer <> 3 THEN 'L1800->U2100'
						WHEN S.Layer = 8 AND N.Layer = 3 THEN 'L1800->U900'
						WHEN S.Layer = 9 AND N.Layer <> 3 THEN 'L2100->U2100'
						WHEN S.Layer = 9 AND N.Layer = 3 THEN 'L2100->U900'
						WHEN S.Layer = 10 AND N.Layer <> 3 THEN 'L2600->U2100'
						WHEN S.Layer = 10 AND N.Layer = 3 THEN 'L2600->U900'
					END)
				ELSE 'NOT_Co-Cell'

			END
			AS CoCell_RelationType,
			CASE WHEN S.Sector_ID = N.Sector_ID 
				THEN (	CASE WHEN T.SRCNAME_NBRNAME IS NULL
						THEN 'Missing'
						ELSE 'OK'
					END)
				ELSE 'N/A'
			END
			AS Missing_L2U_CoCell,
			CASE WHEN S.Sector_ID = N.Sector_ID AND S.Layer <> 7
				THEN (	CASE WHEN N.Layer<>3 
					THEN (	CASE WHEN T.BlindHoPriority='16'
							THEN 'OK'
							ELSE 'NOK'
						END)
					ELSE (	CASE WHEN T.BlindHoPriority='15'
							THEN 'OK'
							ELSE 'NOK'
						END)
					END)
				WHEN S.Sector_ID = N.Sector_ID AND S.Layer = 7
					THEN (	CASE WHEN N.Layer<>3 
						THEN (	CASE WHEN T.BlindHoPriority='16'
								THEN 'OK'
								ELSE 'NOK'
							END)
						ELSE (	CASE WHEN T.BlindHoPriority='17'
								THEN 'OK'
								ELSE 'NOK'
							END)
						END)
				ELSE (	CASE 	WHEN T.BlindHoPriority='0' THEN 'OK'
							
						WHEN T.BlindHoPriority IS NULL THEN 'N/A'
						
						ELSE 'NOK'
					END)
			END
			AS CHECK_BlindHoPriority
		FROM Cell_4G S
			LEFT OUTER JOIN CELL_3G N ON S.Site = N.Site
			LEFT OUTER JOIN NBRS_L2U T ON CONCAT(S.CellName,'_', N.CELLNAME) = T.SRCNAME_NBRNAME
			LEFT OUTER JOIN batches B ON S.Site = B.Site
		WHERE 	N.CELLID IS NOT NULL AND 
			S.`CellActiveState` = 'CELL_ACTIVE' AND
			N.ACTSTATUS = 'ACTIVATED'
		) AS a
	WHERE 	Relation = 'Co-Sector' AND 
		(
		Missing_L2U_CoCell <> 'OK' OR
		CHECK_BlindHoPriority <> 'OK'
		);	

CREATE TABLE CHECK_MISSING_COSITE_L2LINTRA
	SELECT	`City Cluster`, S.eNodeB, S.ENodeBId, S.CellName, S.CellId, S.`CellActiveState`, N.CellName AS NBRCellName, N.CellId AS NBRCellId, N.`CellActiveState` AS NBR_CellActiveState,
		CONCAT(S.CellName,'_', N.CellName) AS SRCNAME_NBRNAME
	FROM Cell_4G S
		INNER JOIN CELL_4G N ON S.Site = N.Site AND N.Layer = S.Layer
		LEFT OUTER JOIN nbrs_L2LINTRA T ON CONCAT(S.CellName,'_', N.CellName) = T.SRCNAME_NBRNAME
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	T.SRCNAME_NBRNAME IS NULL AND 
		S.CellId <> N.CellId AND
		S.`CellActiveState` = 'CELL_ACTIVE' AND
		N.`CellActiveState` = 'CELL_ACTIVE';	
			
CREATE TABLE CHECK_MISSING_COSITE_L2LINTER
	SELECT	`City Cluster`, S.eNodeB, S.ENodeBId, S.CellName, S.CellId, S.`CellActiveState`, N.CellName AS NBRCellName, N.CellId AS NBRCellId, N.`CellActiveState` AS NBR_CellActiveState,
		CONCAT(S.CellName,'_', N.CellName) AS SRCNAME_NBRNAME
	FROM Cell_4G S
		INNER JOIN CELL_4G N ON S.Site = N.Site AND N.Layer <> S.Layer
		LEFT OUTER JOIN NBRS_L2LINTER T ON CONCAT(S.CellName,'_', N.CellName) = T.SRCNAME_NBRNAME
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	T.SRCNAME_NBRNAME IS NULL AND
		S.`CellActiveState` = 'CELL_ACTIVE' AND
		N.`CellActiveState` = 'CELL_ACTIVE';	

*/

# Check one-way neighbours

CREATE OR REPLACE TEMPORARY TABLE nbrs_g2g_temp (INDEX `SRC_CellID` (`SRC_CellID`), INDEX `NBR_CellID` (`NBR_CellID`), INDEX `NBR_Site` (`NBR_Site`), INDEX `NBRCI_SRCCI` (`NBRCI_SRCCI`),
						INDEX `SRCCI_NBRCI` (`SRCCI_NBRCI`), INDEX `SRCCELLNAME` (`SRCCELLNAME`), INDEX `NBRCELLNAME` (`NBRCELLNAME`))
	SELECT *, LEFT(n.`NBRCELLNAME`,4) AS NBR_Site, CONCAT(n.NBR_CellID,'_',n.SRC_CellID) AS NBRCI_SRCCI
	FROM operator.`nbrs_g2g` n
	WHERE n.`NCELLTYPE` <> 'None' AND n.`NCELLSCETYPE` = '00';
	
CREATE TABLE CHECK_oneway_nbrs_g2g		
	SELECT  C.`City Cluster`, G.BSCName AS BSCName_SRC, A.NBRCELLNAME AS CellName_SRC, A.NBR_CellID AS CellID_SRC, s.G2GNCELL_Count AS G2G_Count_SRC,
				A.BSCName AS BSCName_NBR, A.SRCCELLNAME AS CellName_NBR, A.SRC_CellID AS CellID_NBR, t.G2GNCELL_Count AS G2G_Count_NBR,
				A.`NCELLTYPE`, A.`NCELLSCETYPE`,
				n.`H370C:OUTGOING INTER-CELL HANDOVER REQUESTS` AS Inc_HO_Requests, 
				n.`H373:SUCCESSFUL OUTGOING INTER-CELL HANDOVERS` AS Inc_HO_Success,
				n.`H372:FAILED OUTGOING INTER-CELL HANDOVERS` AS Inc_HO_Failures
		FROM nbrs_g2g_temp A
			LEFT OUTER JOIN nbrs_g2g_temp B ON A.NBRCI_SRCCI = B.SRCCI_NBRCI
			LEFT OUTER JOIN cell_2g G ON A.NBR_CellID = G.CI
			LEFT OUTER JOIN batches C ON A.`NBR_Site` = C.Site
			LEFT JOIN operator_kpis.`kpis_2g_nbrs` n ON A.`SRCCELLNAME`= n.`Cell` AND A.`NBRCELLNAME` = n.`TARGET CELL NAME`
			LEFT JOIN operator.`check_count_g2gncell` s ON A.NBR_CellID = s.CI
			LEFT JOIN operator.`check_count_g2gncell` t ON A.SRC_CellID = t.CI
			
		WHERE B.SRCCI_NBRCI IS NULL;
		
CREATE OR REPLACE TEMPORARY TABLE nbrs_u2uintra_temp (INDEX `CELLNAME` (`CELLNAME`), INDEX `NCELLNAME` (`NCELLNAME`), INDEX `Site` (`Site`), INDEX `CELLID` (`CELLID`), 
							INDEX `NCELLID` (`NCELLID`), INDEX `SRCCI_NBRCI` (`SRCCI_NBRCI`), INDEX `NBRCI_SRCCI` (`NBRCI_SRCCI`))
	SELECT *, LEFT(`NCELLNAME`,4) AS Site, CONCAT(NCellID,'_',CellID) AS NBRCI_SRCCI FROM operator.nbrs_u2uintra;

CREATE TABLE CHECK_oneway_nbrs_u2uintra	
	SELECT  c.`City Cluster`, A.NCELLRNCID AS RNCId_SRC, A.NCELLNAME AS CellName_SRC, A.NCELLID AS CellID_SRC, s.`UINTRAFREQNCELL_Count` AS U2Uintra_count_SRC,
		A.RNCID AS RNCId_NBR, A.CELLNAME AS CellName_NBR, A.CellID AS CellID_NBR, t.`UINTRAFREQNCELL_Count` AS U2Uintra_count_NBR,
		l.`VS#SHO#ADDRLATT#NCELL` AS Out_SHO_Att, k.`VS#SHO#ADDRLATT#NCELL` AS Inc_SHO_Att
		FROM nbrs_u2uintra_temp A
			LEFT OUTER JOIN nbrs_u2uintra_temp B ON A.NBRCI_SRCCI = B.SRCCI_NBRCI
			LEFT OUTER JOIN batches C ON a.Site = c.Site
			LEFT JOIN operator_kpis.`kpis_3g_nbrs` k ON A.`CELLNAME` = k.`CELL NAME` AND A.NCELLNAME = k.`DEST CELL NAME`
			LEFT JOIN operator_kpis.`kpis_3g_nbrs` l ON A.`NCELLNAME` = l.`CELL NAME` AND A.CELLNAME = l.`DEST CELL NAME`
			LEFT JOIN operator.`check_count_uintrafreqncell` s ON s.`CELLID` = A.`NCELLID`
			LEFT JOIN operator.`check_count_uintrafreqncell` t ON t.`CELLID` = A.`CELLID`
		WHERE B.SRCCI_NBRCI IS NULL;


# Check clashes
DROP TABLE IF EXISTS inc_ho_failures;
DROP TABLE IF EXISTS inc_sho_failures;
DROP TABLE IF EXISTS inc_hho_failures;
DROP TABLE IF EXISTS CHECK_Clash_2G_BCCHBSIC;
DROP TABLE IF EXISTS CHECK_Clash_3G_PSC;
DROP TABLE IF EXISTS CHECK_Clash_4G_PCI;
DROP TABLE IF EXISTS gcell_sfh_data_geo;
DROP TABLE IF EXISTS check_clash_2g_sfh_hsn;

# 2G SFH HSN clash check
CREATE TEMPORARY TABLE `gcell_sfh_data_geo` (INDEX Site (`Site`), INDEX HSN (`HSN`), INDEX HopBand (`HopBand`), INDEX Latitude (`Latitude`), INDEX Longitude (`Longitude`))
	SELECT s.`BSCName`, c.`SiteID`, s.`Site`, s.`CELLNAME`, s.`HOPINDEX`, s.`HSN`, s.`TRXID`, s.`TRXMAIO`, s.`HopBand`, s.`Min_SFH_Freq`, s.`Max_SFH_Freq`, c.`Latitude`, c.`Longitude`
	FROM `gcell_sfh_data` s
		JOIN `2g_cells_ready` c ON s.`Site` = c.`Site`;
CREATE TABLE check_clash_2g_sfh_hsn	
	SELECT DISTINCT	B.`City Cluster`, S.BSCName, S.`SiteID`, S.Site, S.`HopBand`, S.`HSN`, S.`Min_SFH_Freq`, S.`Max_SFH_Freq`,
		T.BSCName AS Target_BSCName, T.`SiteID` AS Target_SiteID, T.Site AS Target_Site, S.`Min_SFH_Freq` AS Target_Min_SFH_Freq, S.`Max_SFH_Freq` AS Target_Max_SFH_Freq,
		distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude) AS DISTANCE
	FROM `gcell_sfh_data_geo` S
		INNER JOIN `gcell_sfh_data_geo` T ON S.`HSN` = T.`HSN` AND S.`HopBand` = T.`HopBand`
		LEFT OUTER JOIN batches B ON S.Site = B.Site
	WHERE 	S.`Site` <> T.`Site` AND 
		distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<20 # GSM: check until 40km distance
	ORDER BY DISTANCE;

# 2G BCCH&BSIC clash check
CREATE TEMPORARY TABLE inc_ho_failures
	SELECT 	ci, cell, SUM(`H380:INCOMING INTER-CELL HANDOVER REQUESTS`) AS Inc_HO_Requests, 
		SUM(`H382:FAILED INCOMING INTER-CELL HANDOVERS`) AS Inc_HO_Fails, 
		ROUND(SUM(`H382:FAILED INCOMING INTER-CELL HANDOVERS`)/SUM(`H380:INCOMING INTER-CELL HANDOVER REQUESTS`)*100, 1)
	AS Inc_HO_Fail_rate
	FROM operator_kpis.`kpis_2g_nbrs`
	GROUP BY ci, cell;
	
CREATE TABLE CHECK_Clash_2G_BCCHBSIC	
	SELECT 	`City Cluster`, S.BSCName, S.SiteID, S.Site, S.Site_Name, S.CELLNAME, S.CELLID, S.ACTSTATUS, S.BCCH, S.BSIC, 
		T.BSCName AS Target_BSCName, T.SiteID AS Target_SiteId, T.Site AS Target_Site, T.Site_Name AS Target_Site_Name, T.CELLNAME AS Target_CELLNAME, 
		T.CELLID AS Target_CELLID, T.ACTSTATUS AS Target_ACTSTATUS, 
		distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude) AS DISTANCE,
		K.Inc_HO_Fail_rate AS Source_Inc_HO_Fail_rate, K.Inc_HO_Fails AS Source_Inc_HO_Fails,
		L.Inc_HO_Fail_rate AS Target_Inc_HO_Fail_rate, L.Inc_HO_Fails AS Target_Inc_HO_Fails
	FROM 2g_cells_ready S
		INNER JOIN 2g_cells_ready T ON S.BCCH = T.BCCH AND S.BSIC = T.BSIC
		LEFT OUTER JOIN batches B ON S.Site = B.Site
		LEFT OUTER JOIN inc_ho_failures	AS K ON K.ci = S.`CELLID`
		LEFT OUTER JOIN inc_ho_failures	AS L ON L.ci = T.`CELLID`
	WHERE 	S.CELLID <> T.CELLID AND 
		S.ACTSTATUS = 'ACTIVATED' AND
		T.ACTSTATUS = 'ACTIVATED' AND
		distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<40 # GSM: check until 40km distance
	ORDER BY Source_Inc_HO_Fail_rate*Source_Inc_HO_Fails DESC, DISTANCE;


# 3G PSC clash check
CREATE TEMPORARY TABLE inc_sho_failures
	SELECT `DEST RNC ID` AS RNCID, `DEST CELL ID` AS CELLID , `DEST CELL NAME` AS CELLNAME, SUM(`VS#SHO#FAILASU#NOREPLY#NCELL`) AS Inc_SHO_fail_NoReply,
		SUM(`VS#SHO#ATTASU#NCELL`) AS Inc_SHO_ATT, 
		SUM(`VS#SHO#SUCCASU#NCELL`) AS Inc_SHO_SUCC, 
		SUM(`VS#SHO#ATTASU#NCELL`) - SUM(`VS#SHO#SUCCASU#NCELL`) AS Inc_SHO_Fails,
		ROUND((SUM(`VS#SHO#ATTASU#NCELL`) - SUM(`VS#SHO#SUCCASU#NCELL`))/SUM(`VS#SHO#ATTASU#NCELL`)*100,3) AS Inc_SHO_Fail_Rate
	FROM operator_kpis.`kpis_3g_nbrs`
	GROUP BY `DEST RNC ID`, `DEST CELL ID`, `DEST CELL NAME`;
	
CREATE TABLE CHECK_Clash_3G_PSC	
	SELECT 	`City Cluster`, S.DL_UARFCN, S.LOGICRNCID, S.SiteID, S.Site, S.CELLNAME, S.CELLID, S.PSC, S.ACTSTATUS, 
		T.LOGICRNCID AS Target_RNCID, T.SiteID AS Target_SiteID, T.Site AS Target_Site, T.CELLNAME AS Target_CELLNAME, 
		T.CELLID AS Target_CELLID, T.ACTSTATUS AS Target_ACTSTATUS, 
		distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude) AS DISTANCE,
		K.Inc_SHO_Fail_Rate AS Source_Inc_SHO_Fail_rate, K.Inc_SHO_Fails AS Source_Inc_SHO_Fails, K.Inc_SHO_fail_NoReply AS Source_Inc_SHO_Fail_NoReply,
		L.Inc_SHO_Fail_Rate AS Target_Inc_SHO_Fail_rate, L.Inc_SHO_Fails AS Target_Inc_SHO_Fails, L.Inc_SHO_fail_NoReply AS Target_Inc_SHO_Fail_NoReply
	FROM 3g_cells_ready S
		INNER JOIN 3g_cells_ready T ON S.PSC = T.PSC AND S.Layer = T.Layer
		LEFT OUTER JOIN batches B ON S.Site = B.Site
		LEFT OUTER JOIN inc_sho_failures K ON K.CELLID = S.CELLID
		LEFT OUTER JOIN inc_sho_failures L ON L.CELLID = T.CELLID
	WHERE 	S.CELLID <> T.CELLID AND 
		S.ACTSTATUS = 'ACTIVATED' AND
		T.ACTSTATUS = 'ACTIVATED' AND
		(CASE 	WHEN S.DL_UARFCN < 10000 # not 2100MHz channels
			THEN distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<40 # not U2100: check until 40km distance
			ELSE distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<25 # U2100: check until 25km distance
		END)
	ORDER BY Source_Inc_SHO_Fail_rate*Source_Inc_SHO_Fails DESC, DISTANCE;


# 4G PCI clash check				
CREATE TABLE inc_hho_failures
	SELECT `ENODEB NAME` AS eNodeB,`LOCAL ENODEB IDENTITY` AS eNodeBId,`LOCAL CELL IDENTITY` AS Local_cell,`LOCAL CELL NAME` AS CellName,
		SUM(`L#HHO#NCELL#EXECATTIN`) AS Inc_HHO_attempts,
		SUM(`L#HHO#NCELL#EXECATTIN`) - SUM(`L#HHO#NCELL#EXECSUCCIN`) AS Inc_HHO_fails,
		ROUND((SUM(`L#HHO#NCELL#EXECATTIN`) - SUM(`L#HHO#NCELL#EXECSUCCIN`))/SUM(`L#HHO#NCELL#EXECATTIN`)*100,3) AS Inc_HHO_Fail_rate
	FROM operator_kpis.`kpis_4g_nbrs`
	GROUP BY `ENODEB NAME`, `LOCAL ENODEB IDENTITY`, `LOCAL CELL IDENTITY`, `LOCAL CELL NAME`;
	
CREATE TABLE CHECK_Clash_4G_PCI
	SELECT 	`City Cluster`, S.DlEarfcn, S.eNodeB,  S.Site, S.CellId, S.CELLNAME, S.PCI, S.CellActiveState, 
		T.eNodeB AS Target_eNodeB, T.Site AS Target_Site, T.CellId AS Target_CellId, T.CELLNAME AS Target_CELLNAME, 
		T.CellActiveState AS Target_CellActiveState, distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude) AS DISTANCE,
		K.Inc_HHO_Fail_rate AS Source_Inc_HHO_Fail_rate, K.Inc_HHO_fails AS Source_Inc_HHO_Fails,
		L.Inc_HHO_Fail_rate AS Target_Inc_HHO_Fail_rate, L.Inc_HHO_fails AS Target_Inc_HHO_Fails
	FROM 4g_cells_ready S
		INNER JOIN 4g_cells_ready T ON S.PCI = T.PCI AND S.Layer = T.Layer
		LEFT OUTER JOIN batches B ON S.Site = B.Site
		LEFT OUTER JOIN inc_hho_failures K ON K.`eNodeBId` = S.Site AND K.Local_cell = S.`LocalCellId`
		LEFT OUTER JOIN inc_hho_failures L ON L.`eNodeBId` = T.Site AND L.Local_cell = T.`LocalCellId`
	WHERE 	S.Site_LOCELLID <> T.Site_LOCELLID AND 
		S.CellActiveState = 'CELL_ACTIVE' AND
		T.CellActiveState = 'CELL_ACTIVE' AND
		(CASE 	WHEN S.FreqBand = 8 THEN distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<40 # L900: check until 40km distance
			WHEN S.FreqBand = 7 THEN distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<20 # L2600: check until 20km distance
			WHEN S.FreqBand IN (1,3) THEN distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<25 # L1800&L2100: check until 25km distance
			ELSE distcalc(S.Latitude, S.Longitude, T.Latitude, T.Longitude)<30 # other LTE bands: check until 30km distance
		END)
	ORDER BY Source_Inc_HHO_Fail_rate*Source_Inc_HHO_Fails DESC, DISTANCE;


# Check planned ASSET el. tilt vs configured RET tilt

DROP TABLE IF EXISTS check_ret_tilt_diff_2g;
DROP TABLE IF EXISTS check_ret_tilt_diff_3g;
DROP TABLE IF EXISTS check_ret_tilt_diff_4g;

CREATE TABLE check_ret_tilt_diff_2g
	SELECT 	`City Cluster`,`SiteID`,`Site_Name`,l.`Site`,`BSCName`,`CELLID`,`CELLNAME`,`Sector_ID`,`Azimuth`,`Antenna`,`Mech_Tilt`,`El_Tilt`,
		`RET_Label`,`RET_Tilt`,
		CASE 	WHEN LOCATE('MULTI_RET', RET_Label) > 0
			THEN 	'MULTI_RET'
			ELSE 	CASE 	WHEN LOCATE(';', RET_Tilt) > 0
					THEN 	CASE 	WHEN SUBSTRING_INDEX(RET_Tilt,';',1)>100
							THEN 'N/A'
							ELSE CONCAT((SUBSTRING_INDEX(RET_Tilt,';',1) - El_Tilt),';',(SUBSTRING_INDEX(RET_Tilt,';',-1) - El_Tilt))
						END
					ELSE 	CASE 	WHEN RET_Tilt>100 
							THEN 'N/A'
							ELSE (RET_Tilt - El_Tilt) 
						END	
				END
		END	
		AS ElTilt_Diff
	FROM `2g_cells_ready` l
		LEFT OUTER JOIN `batches` b ON l.`Site` = b.`Site`
	WHERE 	`RET_Tilt` IS NOT NULL AND 
		`El_Tilt` IS NOT NULL AND
		CAST(`El_Tilt` AS DOUBLE) <> CAST(LEFT(`RET_Tilt`,3) AS DOUBLE)
	ORDER BY 	
		CASE 	WHEN ElTilt_Diff IN ('N/A', 'MULTI_RET') 
			THEN 100
			ELSE 	CASE 	WHEN LOCATE(';', ElTilt_Diff)
					THEN CAST(SUBSTRING_INDEX(ElTilt_Diff,';',1) AS DOUBLE)
					ELSE CAST(LEFT(ElTilt_Diff,3) AS DOUBLE)
				END
		END;

CREATE TABLE check_ret_tilt_diff_3g
	SELECT 	`City Cluster`,`SiteID`,l.`Site`,`LOGICRNCID`,`CELLID`,`CELLNAME`,`Sector_ID`,`DL_UARFCN`,`Azimuth`,`Antenna`,`Mech_Tilt`,`El_Tilt`,
		`RET_Label`,`RET_Tilt`,
		CASE 	WHEN LOCATE('MULTI_RET', RET_Label) > 0
			THEN 	'MULTI_RET'
			ELSE 	CASE 	WHEN LOCATE(';', RET_Tilt) > 0
					THEN 	CASE 	WHEN SUBSTRING_INDEX(RET_Tilt,';',1)>100
							THEN 'N/A'
							ELSE CONCAT((SUBSTRING_INDEX(RET_Tilt,';',1) - El_Tilt),';',(SUBSTRING_INDEX(RET_Tilt,';',-1) - El_Tilt))
						END
					ELSE 	CASE 	WHEN RET_Tilt>100 
							THEN 'N/A'
							ELSE (RET_Tilt - El_Tilt) 
						END	
				END
		END	
		AS ElTilt_Diff
	FROM `3g_cells_ready` l
		LEFT OUTER JOIN `batches` b ON l.`Site` = b.`Site`
	WHERE 	`RET_Tilt` IS NOT NULL AND 
		`El_Tilt` IS NOT NULL AND
		CAST(`El_Tilt` AS DOUBLE) <> CAST(LEFT(`RET_Tilt`,3) AS DOUBLE)
	ORDER BY 	
		CASE 	WHEN ElTilt_Diff IN ('N/A', 'MULTI_RET') 
			THEN 100
			ELSE 	CASE 	WHEN LOCATE(';', ElTilt_Diff)
					THEN CAST(SUBSTRING_INDEX(ElTilt_Diff,';',1) AS DOUBLE)
					ELSE CAST(LEFT(ElTilt_Diff,3) AS DOUBLE)
				END
		END;

CREATE TABLE check_ret_tilt_diff_4g
	SELECT 	`City Cluster`, `eNodeB`, `SiteID`, l.`Site`, `LocalCellId`, `Site_LOCELLID`,`Sector_ID`,`CellName`,`DlEarfcn`, `Azimuth`, `Antenna`,
		`Mech_Tilt`,`El_Tilt`,`RET_Label`,`RET_Tilt`,
		CASE 	WHEN LOCATE('MULTI_RET', RET_Label) > 0
			THEN 	'MULTI_RET'
			ELSE 	CASE 	WHEN LOCATE(';', RET_Tilt) > 0
					THEN 	CASE 	WHEN SUBSTRING_INDEX(RET_Tilt,';',1)>100
							THEN 'N/A'
							ELSE CONCAT((SUBSTRING_INDEX(RET_Tilt,';',1) - El_Tilt),';',(SUBSTRING_INDEX(RET_Tilt,';',-1) - El_Tilt))
						END
					ELSE 	CASE 	WHEN RET_Tilt>100 
							THEN 'N/A'
							ELSE (RET_Tilt - El_Tilt) 
						END	
				END
		END	
		AS ElTilt_Diff
	FROM `4g_cells_ready` l
		LEFT OUTER JOIN `batches` b ON l.`Site` = b.`Site`
	WHERE 	`RET_Tilt` IS NOT NULL AND 
		`El_Tilt` IS NOT NULL AND
		CAST(`El_Tilt` AS DOUBLE) <> CAST(LEFT(`RET_Tilt`,3) AS DOUBLE)
	ORDER BY 	
		CASE 	WHEN ElTilt_Diff IN ('N/A', 'MULTI_RET') 
			THEN 100
			ELSE 	CASE 	WHEN LOCATE(';', ElTilt_Diff)
					THEN CAST(SUBSTRING_INDEX(ElTilt_Diff,';',1) AS DOUBLE)
					ELSE CAST(LEFT(ElTilt_Diff,3) AS DOUBLE)
				END
		END;
	
# Check 4g cell customized bandwidth 

DROP TABLE IF EXISTS check_4g_cell_bw;

CREATE TABLE check_4g_cell_bw
	SELECT `City Cluster`,`eNodeB`,`SiteID`,l.Site,`Sector_ID`,`LocalCellId`,`CellName`,`Site_LOCELLID`,`DlEarfcn`,`UlBandWidth`,`DlBandWidth`,`CustomizedULBandWidth`,`CustomizedDLBandWidth`
	FROM `4g_cells_ready` l
	LEFT OUTER JOIN `batches` b ON l.`Site` = b.`Site`
	WHERE `CustomizedBandWidthCfgInd` = 'CFG';
	
# Check co-sector LTE1800 vs LTE2100 mean timing advance difference

DROP TABLE IF EXISTS mean_ta_lte1800;
DROP TABLE IF EXISTS mean_ta_lte2100;
DROP TABLE IF EXISTS check_4g_cosector_mean_ta_diff;

CREATE TEMPORARY TABLE mean_ta_lte1800
	SELECT `Date`, `eNodeB Name`, MID(`CellName`,5,1) AS Sector_ID, RIGHT(`CellName`,1) AS Layer, `CellName`, 
		`Ctel_perc_L_RA_TA_Total(number)` AS Total_TA_Samples,
	(
		ROUND(
		(`Ctel_perc_L_RA_TA_UE_Index0`*78.12+
		`Ctel_perc_L_RA_TA_UE_Index1`*234.36+
		`Ctel_perc_L_RA_TA_UE_Index2`*546.84+
		`Ctel_perc_L_RA_TA_UE_Index3`*1015.56+
		`Ctel_perc_L_RA_TA_UE_Index4`*1953+
		`Ctel_perc_L_RA_TA_UE_Index5`*3515.4+
		`Ctel_perc_L_RA_TA_UE_Index6`*6640.2+
		`Ctel_perc_L_RA_TA_UE_Index7`*14452.2+
		`Ctel_perc_L_RA_TA_UE_Index8`*30076.2+
		`Ctel_perc_L_RA_TA_UE_Index9`*53512.2+
		`Ctel_perc_L_RA_TA_UE_Index10`*76948.2+
		`Ctel_perc_L_RA_TA_UE_Index11`*100149.84)/100
		,1)
	) AS Mean_LTE_TA
	FROM `operator_kpis`.`propagation_4g`
	WHERE `Date` = (SELECT MAX(`Date`) FROM `operator_kpis`.`propagation_4g`) AND RIGHT(`CellName`,1) = '7';
	
CREATE TEMPORARY TABLE mean_ta_lte2100
	SELECT `Date`, `eNodeB Name`, MID(`CellName`,5,1) AS Sector_ID, RIGHT(`CellName`,1) AS Layer, `CellName`,
		`Ctel_perc_L_RA_TA_Total(number)` AS Total_TA_Samples,
	(
		ROUND(		
		(`Ctel_perc_L_RA_TA_UE_Index0`*78.12+
		`Ctel_perc_L_RA_TA_UE_Index1`*234.36+
		`Ctel_perc_L_RA_TA_UE_Index2`*546.84+
		`Ctel_perc_L_RA_TA_UE_Index3`*1015.56+
		`Ctel_perc_L_RA_TA_UE_Index4`*1953+
		`Ctel_perc_L_RA_TA_UE_Index5`*3515.4+
		`Ctel_perc_L_RA_TA_UE_Index6`*6640.2+
		`Ctel_perc_L_RA_TA_UE_Index7`*14452.2+
		`Ctel_perc_L_RA_TA_UE_Index8`*30076.2+
		`Ctel_perc_L_RA_TA_UE_Index9`*53512.2+
		`Ctel_perc_L_RA_TA_UE_Index10`*76948.2+
		`Ctel_perc_L_RA_TA_UE_Index11`*100149.84)/100
		,1)
	) AS Mean_LTE_TA
	FROM `operator_kpis`.`propagation_4g`
	WHERE `Date` = (SELECT MAX(`Date`) FROM `operator_kpis`.`propagation_4g`) AND RIGHT(`CellName`,1) = '8';

CREATE TABLE check_4g_cosector_mean_ta_diff
	SELECT 	c.`City Cluster`, a.`Date`, a.`eNodeB Name`, a.Sector_ID,
		a.`CellName` AS CellName_L1800, l.`Antenna` AS AntennaType_L1800, l.Mech_Tilt AS MT_L1800, l.El_Tilt AS ET_L1800,
		l.`RET_Tilt` AS RET_Tilt_L1800, a.Total_TA_Samples AS TA_Samples_L1800, a.Mean_LTE_TA AS Mean_LTE_TA_L1800, 
		b.Mean_LTE_TA AS Mean_LTE_TA_L2100, b.Total_TA_Samples AS TA_Samples_L2100, h.`RET_Tilt` AS RET_Tilt_L2100,
		h.El_Tilt AS ET_L2100, h.Mech_Tilt AS MT_L2100, h.`Antenna` AS AntennaType_L2100, b.`CellName` AS CellName_L2100,
		CASE 	WHEN l.RET_Tilt IS NOT NULL AND h.RET_Tilt IS NOT NULL 
				THEN CAST((h.Mech_Tilt+
					CASE WHEN LOCATE(';',h.RET_Tilt)>0 
						THEN LEFT(h.RET_Tilt, LOCATE(';',h.RET_Tilt)-1)
						ELSE h.RET_Tilt
					END
					) - 
					(l.Mech_Tilt+
					CASE WHEN LOCATE(';',l.RET_Tilt)>0 
						THEN LEFT(l.RET_Tilt, LOCATE(';',l.RET_Tilt)-1)
						ELSE l.RET_Tilt
					END) 
					AS DECIMAL(3,1))
			WHEN l.RET_Tilt IS NULL AND h.RET_Tilt IS NOT NULL 
				THEN CAST((h.Mech_Tilt+
					CASE WHEN LOCATE(';',h.RET_Tilt)>0 
						THEN LEFT(h.RET_Tilt, LOCATE(';',h.RET_Tilt)-1)
						ELSE h.RET_Tilt
					END
					) - 
					(l.Mech_Tilt+l.El_Tilt) AS DECIMAL(3,1))
			WHEN l.RET_Tilt IS NOT NULL AND h.RET_Tilt IS NULL 
				THEN CAST((h.Mech_Tilt+h.El_Tilt) - 
					(l.Mech_Tilt+
					CASE WHEN LOCATE(';',l.RET_Tilt)>0 
						THEN LEFT(l.RET_Tilt, LOCATE(';',l.RET_Tilt)-1)
						ELSE l.RET_Tilt
					END) AS DECIMAL(3,1))
			ELSE CAST((h.Mech_Tilt+h.El_Tilt) - (l.Mech_Tilt+l.El_Tilt) AS DECIMAL(3,1))
		END
		AS Total_Tilt_diff,
		((b.Mean_LTE_TA - a.Mean_LTE_TA)/a.Mean_LTE_TA)*100 AS Mean_TA_diff_perc
	FROM  mean_ta_lte1800 a 
		JOIN mean_ta_lte2100 b ON a.`eNodeB Name`= b.`eNodeB Name` AND a.Sector_ID = b.Sector_ID
		LEFT JOIN operator.`4g_cells_ready` l ON a.`eNodeB Name` = l.SiteID AND a.`CellName` = l.`CellName`
		LEFT JOIN operator.`4g_cells_ready` h ON b.`eNodeB Name` = h.SiteID AND b.`CellName` = h.`CellName`
		LEFT JOIN operator.`batches` c ON MID(a.`eNodeB Name`,3,4) = c.Site
	WHERE ABS(((b.Mean_LTE_TA - a.Mean_LTE_TA)/a.Mean_LTE_TA)*100) > 40 # TA difference between L1800 and L2100 is more than 40%
	ORDER BY ((b.Mean_LTE_TA - a.Mean_LTE_TA)/a.Mean_LTE_TA)*100 DESC;

# Check Asset planning vs live network

DROP TABLE IF EXISTS check_planning_2g;
DROP TABLE IF EXISTS check_planning_3g;
DROP TABLE IF EXISTS check_planning_4g;

CREATE TABLE check_planning_2g
	SELECT DISTINCT	b.`City Cluster`, g.`BSCName`, g.`SiteID`, g.`Site`, g.`Sector_ID`, g.`CELLNAME`, g.`CELLID`, g.`BCCH`, a.`BCCH` AS 'Asset_BCCH', 
		g.`BSIC`, CONCAT(a.`NCC`, a.`BCC`) AS 'Asset_BSIC', 
		CASE WHEN g.`BCCH` <> a.`BCCH`
			THEN 'NOK'
			ELSE 'OK'
		END
		AS 'Check_BCCH',
		CASE WHEN g.`BSIC` <> CONCAT(a.`NCC`, a.`BCC`) 
			THEN 'NOK'
			ELSE 'OK'
		END
		AS 'Check_BSIC'
	FROM `2g_cells_ready` g
		LEFT JOIN `asset_2g` a ON a.`GSM ID` = g.`CELLID`
		LEFT JOIN `batches` b ON b.Site = g.`Site`
	WHERE g.`BCCH` <> a.`BCCH` 
	ORDER BY g.`Site`, g.`Sector_ID`, g.`CellName`;

CREATE TABLE check_planning_3g
	SELECT DISTINCT	b.`City Cluster`, u.`LOGICRNCID`, u.`SiteID`, u.`Site`, u.`Sector_ID`, u.`CELLNAME`, u.`CELLID`, u.`PSC`, a.`PSC` AS 'Asset_PSC',
		CASE WHEN u.`PSC` <> a.`PSC`
			THEN 'NOK'
			ELSE 'OK'
		END
		AS 'Check_PSC'
	FROM `3g_cells_ready` u
		LEFT JOIN `asset_3g` a ON a.`Cell ID` = u.`CELLID`
		LEFT JOIN `batches` b ON b.Site = u.`Site`
	WHERE u.`PSC` <> a.`PSC` 
	ORDER BY u.`Site`, u.`Sector_ID`, u.`CellName`;

CREATE TABLE check_planning_4g
	SELECT DISTINCT	b.`City Cluster`, l.`SiteID`, l.`Site`, l.`Sector_ID`, l.`CellName`, l.`PCI`, a.`PCI` AS 'Asset_PCI', l.`RootSequenceIdx`, a.`First Assigned RSI` AS 'Asset_RSI',
		CASE WHEN l.`PCI` <> a.`PCI`
			THEN 'NOK'
			ELSE 'OK'
		END
		AS 'Check_PCI',
			CASE WHEN l.`RootSequenceIdx` <> a.`First Assigned RSI`
			THEN 'NOK'
			ELSE 'OK'
		END
		AS 'Check_RSI'

	FROM `4g_cells_ready` l
		LEFT JOIN `asset_4g` a ON a.`Cell Name` = l.`CellName`
		LEFT JOIN `batches` b ON b.site = l.site
	WHERE l.`PCI` <> a.`PCI` OR l.`RootSequenceIdx` <> a.`First Assigned RSI`
	ORDER BY l.`Site`, l.`Sector_ID`, l.`CellName`;


# Check 4G CAGROUPCELLCFG

CREATE OR REPLACE TEMPORARY TABLE ca_temp1
	SELECT 	c.`City Cluster`, a.`eNodeB`, a.`SiteID`, a.`Site`, a.`Sector_ID`, d.`CoSector_Layers`,
		a.`LocalCellId`, a.`CellName`, a.`DlEarfcn`, 
		b.`LocalCellId` AS SCell_LocalCellId, b.`CellName` AS SCell_Cellname, b.`DlEarfcn` AS SCell_DlEarfcn,
		CASE 	WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 3 THEN 'L21-L18'
			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 7 THEN 'L21-L26'
			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 8 THEN 'L21-L9'
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 1 THEN 'L18-L21'
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 7 THEN 'L18-L26'
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 8 THEN 'L18-L9'
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 1 THEN 'L26-L21'
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 3 THEN 'L26-L18'
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 8 THEN 'L26-L9'
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 1 THEN 'L9-L21'
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 3 THEN 'L9-L18'
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 7 THEN 'L9-L26'
		END
		AS 'CA_Link',
		CASE 	WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 3 THEN 'TRUE'
			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 7 THEN 'FALSE'
			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 8 
				THEN 
					CASE WHEN ((LOCATE('L9/L21',d.`CoSector_Layers`)>0 OR LOCATE('L21/L9',d.`CoSector_Layers`)>0) AND
							(LOCATE('L26',d.`CoSector_Layers`)=0 AND LOCATE('L18',d.`CoSector_Layers`)=0))
						THEN 'TRUE'
						ELSE 'FALSE'
					END
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 1 THEN 'TRUE'
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 7 THEN 'FALSE'
			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 8 
				THEN
					CASE WHEN ((LOCATE('L18/L9',d.`CoSector_Layers`)>0 OR LOCATE('L9/L18',d.`CoSector_Layers`)>0) AND
							(LOCATE('L26',d.`CoSector_Layers`)=0 AND LOCATE('L21',d.`CoSector_Layers`)=0))
						THEN 'TRUE'
						ELSE 'FALSE'
					END				
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 1 THEN 'TRUE'
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 3 THEN 'TRUE'
			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 8 
				THEN
					CASE WHEN ((LOCATE('L26/L9',d.`CoSector_Layers`)>0 OR LOCATE('L9/L26',d.`CoSector_Layers`)>0) AND
							(LOCATE('L18',d.`CoSector_Layers`)=0 AND LOCATE('L21',d.`CoSector_Layers`)=0))
						THEN 'TRUE'
						ELSE 'FALSE'
					END	
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 1 THEN 'FALSE'
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 3 THEN 'FALSE'
			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 7 THEN 'FALSE'
		END
		AS 'Blind_Addition'
#		CASE 	WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 3 THEN '2'
#			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 7 THEN '3'
#			WHEN a.`FreqBand` = 1 AND b.`FreqBand` = 8 THEN '1'
#			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 1 THEN '2'
#			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 7 THEN '3'
#			WHEN a.`FreqBand` = 3 AND b.`FreqBand` = 8 THEN '1'
#			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 1 THEN '2'
#			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 3 THEN '2'
#			WHEN a.`FreqBand` = 7 AND b.`FreqBand` = 8 THEN '1'
#			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 1 THEN '1'
#			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 3 THEN '2'
#			WHEN a.`FreqBand` = 8 AND b.`FreqBand` = 7 THEN '3'
#		END
#		AS 'SCell_Priority'
		
	FROM `4g_cells_ready` a
		JOIN `4g_cells_ready` b ON a.`Site` = b.`Site` AND a.`Sector_ID` = b.`Sector_ID`
		LEFT JOIN `batches` c ON c.`Site` = a.`Site`
		LEFT JOIN `cosector_layers` d ON d.Site = a.Site AND d.Sector_ID = a.Sector_ID
	WHERE a.`DlEarfcn` <> b.`DlEarfcn`;

CREATE OR REPLACE TEMPORARY TABLE precheck_4g_cagroupscellcfg
	SELECT 	b.`City Cluster`, l.`eNodeB`, c.Sector_ID, c.CoSector_Layers, l.`Local cell ID`, l.`SCell eNodeB ID`, l.`SCell Local Cell ID`, l.`SCell Blind Configuration Flag`, l.`SCell Priority`, 
		l.`SCell A4 Offset(dB)`, l.`SCell A2 Offset(dB)`, l.`SPID Group ID`, c.CA_link,
		CASE WHEN MID(l.`eNodeB`,3,4) <> l.`SCell eNodeB ID` 
			THEN 'NOK_eNodeB_mismatch'
			ELSE 
				CASE WHEN l.`SCell Blind Configuration Flag` <> c.Blind_Addition 
					THEN CONCAT('NOK_BlindConfigFlag<>', c.Blind_Addition)
					ELSE 'OK'
				END
		END
		AS 'Check_CA'
	FROM `lcagroupscellcfg` l
		LEFT JOIN ca_temp1 c ON l.`eNodeB` = c.`eNodeB` AND l.`Local cell ID` = c.`LocalCellId` AND l.`SCell eNodeB ID` = c.`Site` AND l.`SCell Local Cell ID` = c.SCell_LocalCellId
		LEFT JOIN batches b ON b.Site = MID(l.eNodeB,3,4)
	WHERE l.`eNodeB` NOT LIKE '*eNodeB%';

INSERT INTO precheck_4g_cagroupscellcfg (`City Cluster`, `eNodeB`, `Sector_ID`, `CoSector_Layers`, `Local cell ID`, `SCell eNodeB ID`, `SCell Local Cell ID`, `CA_Link`, 
		`Check_CA`)
	SELECT 	c.`City Cluster`, c.eNodeB, c.Sector_ID, c.CoSector_Layers, c.LocalCellId, c.Site, c.SCell_LocalCellId, c.CA_Link, 'Missing'

	FROM ca_temp1 c
		LEFT JOIN `lcagroupscellcfg` l ON l.`eNodeB` = c.`eNodeB` AND l.`Local cell ID` = c.`LocalCellId` AND l.`SCell eNodeB ID` = c.`Site` AND l.`SCell Local Cell ID` = c.SCell_LocalCellId
		WHERE l.`SCell Local Cell ID` IS NULL AND c.Blind_Addition = 'TRUE';
		
CREATE OR REPLACE TABLE check_4g_cagroupscellcfg 
	SELECT * 
	FROM precheck_4g_cagroupscellcfg
	WHERE Check_CA <> 'OK';


# Check x-feeders by KPI
		
DROP TABLE IF EXISTS temp_crosscheck_att;
DROP TABLE IF EXISTS temp_cosector_layers;
DROP TABLE IF EXISTS temp_crosscheck;
DROP TABLE IF EXISTS check_crosses_by_kpi;

CREATE TEMPORARY TABLE temp_cosector_layers
	SELECT `Site`,`Sector_ID`,`CoSector_Layers`, MID(`CoSector_Layers`,LOCATE('/L',`CoSector_Layers`)+1) AS cosector_LTE_layers
	FROM operator.`cosector_layers`;
	
CREATE TEMPORARY TABLE temp_crosscheck_att
	SELECT 	a.`ENODEB NAME`,
		a.`LOCAL CELL IDENTITY`,
		a.`LOCAL CELL NAME`,
		CASE 	WHEN RIGHT(a.`LOCAL CELL NAME`,1) =  7 THEN '1800'
			WHEN RIGHT(a.`LOCAL CELL NAME`,1) =  8 THEN '2100'
			WHEN RIGHT(a.`LOCAL CELL NAME`,1) =  9 THEN '2600'
			WHEN RIGHT(a.`LOCAL CELL NAME`,1) =  0 THEN '900'
		END
		AS Band, 
		a.`LOCAL ENODEB IDENTITY`, 
		MID(a.`LOCAL CELL NAME`,5,1) AS Local_SectorID, 
		b.cosector_LTE_layers AS Local_cosector_LTE_layers,
		a.`TARGET ENODEB ID`, 
		MID(a.`TARGET CELL NAME`,5,1) AS Target_SectorID, 
		c.cosector_LTE_layers AS Target_cosector_LTE_layers,
		SUM(a.`L#HHO#NCELL#EXECATTOUT`) AS HHO_Att
	FROM operator_kpis.`kpis_4g_nbrs` a
		LEFT JOIN temp_cosector_layers b ON a.`LOCAL ENODEB IDENTITY` = b.Site AND MID(a.`LOCAL CELL NAME`,5,1) = b.Sector_ID
		LEFT JOIN temp_cosector_layers c ON a.`TARGET ENODEB ID` = c.Site AND MID(a.`TARGET CELL NAME`,5,1) = c.Sector_ID
	WHERE a.`LOCAL ENODEB IDENTITY` = a.`TARGET ENODEB ID` AND RIGHT(a.`LOCAL CELL NAME`,1) <> RIGHT(a.`TARGET CELL NAME`,1)
	GROUP BY a.`ENODEB NAME`,a.`LOCAL ENODEB IDENTITY`,a.`LOCAL CELL IDENTITY`,a.`LOCAL CELL NAME`,a.`TARGET ENODEB ID`, Target_SectorID;

CREATE TEMPORARY TABLE temp_crosscheck	
	SELECT 	a.`ENODEB NAME`, a.`LOCAL CELL IDENTITY`, a.`LOCAL CELL NAME`, a.Band, a.`LOCAL ENODEB IDENTITY`, a.Local_SectorID, a.Local_cosector_LTE_layers, 
		a.`TARGET ENODEB ID`, a.Target_SectorID, a.Target_cosector_LTE_layers, a.HHO_Att
	FROM temp_crosscheck_att a
		LEFT JOIN
			(
			SELECT 	`ENODEB NAME`,`LOCAL ENODEB IDENTITY`,`LOCAL CELL IDENTITY`,`LOCAL CELL NAME`, Local_SectorID, Local_cosector_LTE_layers,
				`TARGET ENODEB ID`, MAX(HHO_Att) AS Max_HHO_Att
			FROM temp_crosscheck_att
			GROUP BY `ENODEB NAME`,`LOCAL CELL IDENTITY`,`LOCAL CELL NAME`,`LOCAL ENODEB IDENTITY`, Local_SectorID, Local_cosector_LTE_layers,
				`TARGET ENODEB ID`
			) AS b ON a.`LOCAL CELL NAME` = b.`LOCAL CELL NAME`
	WHERE a.HHO_Att = b.Max_HHO_Att 
		AND a.Target_SectorID <> b.Local_SectorID 
		AND b.Local_cosector_LTE_layers = a.Target_cosector_LTE_layers
		AND a.Local_cosector_LTE_layers IS NOT NULL
		AND a.HHO_Att>0
	ORDER BY a.`LOCAL ENODEB IDENTITY`, a.Band, a.`LOCAL CELL NAME`;

CREATE TABLE check_crosses_by_kpi
	SELECT c.`City Cluster`, a.`ENODEB NAME`, a.`LOCAL CELL NAME`, a.Band, a.`LOCAL ENODEB IDENTITY`, a.Local_SectorID, a.Local_cosector_LTE_layers, 
			a.`TARGET ENODEB ID`, a.Target_SectorID, a.HHO_Att 
	FROM temp_crosscheck a
		LEFT JOIN batches c ON a.`LOCAL ENODEB IDENTITY` = c.Site
		JOIN 
			(
			SELECT `LOCAL ENODEB IDENTITY`
			FROM 	
				(
				SELECT `LOCAL ENODEB IDENTITY`, COUNT(`LOCAL ENODEB IDENTITY`) AS eNodeB_count
				FROM temp_crosscheck
				GROUP BY `LOCAL ENODEB IDENTITY`
				) AS a
			WHERE eNodeB_count>1
			) AS b ON a.`LOCAL ENODEB IDENTITY` = b.`LOCAL ENODEB IDENTITY`;
END$$

DELIMITER ;	