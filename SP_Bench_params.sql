DELIMITER $$

USE `operator_ues`$$

DROP PROCEDURE IF EXISTS `BenchParams`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `BenchParams`(ue_table VARCHAR(100))

BEGIN

SET @ueLabels = CONCAT(ue_table,'_ue_lb');

SET @t = CONCAT('SET @l = (SELECT table_name 
		FROM information_schema.tables 
		WHERE table_name = \'',@ueLabels,'\')');
		
PREPARE stmt FROM @t;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

IF @l IS NOT NULL THEN
	
	DROP TABLE IF EXISTS table_ueLabels;
	
	CREATE TEMPORARY TABLE table_ueLabels (
	     id MEDIUMINT NOT NULL AUTO_INCREMENT,
	     `ue` TEXT, ue_label TEXT, PRIMARY KEY (id));

	#SET @rrc_msgs = 'SystemInformationBlockType1;SystemInformation - SIB2;SystemInformation - SIB3;SystemInformation - SIB2,SIB3;SystemInformation - SIB5;SystemInformation - SIB6;SystemInformation - SIB7;SystemInformation - SIB6,SIB7;';
	
	SET @sib1 = 'mcc,mnc,q-RxLevMin,';
	SET @sib2 = 'numberOfRA-Preambles,sizeOfRA-PreamblesGroupA,messageSizeGroupA,messagePowerOffsetGroupB,powerRampingStep,preambleInitialReceivedTargetPower,preambleTransMax,ra-ResponseWindowSize,mac-ContentionResolutionTimer,maxHARQ-Msg3Tx,prach-ConfigIndex,zeroCorrelationZoneConfig,prach-FreqOffset,referenceSignalPower,p-b,enable64QAM,p0-NominalPUSCH,alpha,p0-NominalPUCCH,t300,t301,t310,n310,t311,n311,ul-CarrierFreq,ul-Bandwidth,';
	SET @sib3 = 'q-Hyst,s-NonIntraSearch,threshServingLow,cellReselectionPriority,q-RxLevMin,s-IntraSearch,presenceAntennaPort1,neighCellConfig,t-ReselectionEUTRA,s-IntraSearchP-r9,s-IntraSearchQ-r9,s-NonIntraSearchP-r9,s-NonIntraSearchQ-r9,q-QualMin-r9,';
	SET @sib5 = 'dl-CarrierFreq,q-RxLevMin,t-ReselectionEUTRA,threshX-High,threshX-Low,allowedMeasBandwidth,presenceAntennaPort1,cellReselectionPriority,neighCellConfig,q-OffsetFreq,q-QualMin-r9,';
	SET @sib6 = 'carrierFreq,cellReselectionPriority,threshX-High,threshX-Low,q-RxLevMin,p-MaxUTRA,q-QualMin,t-ReselectionUTRA,';
	SET @rrcsetup = 'srb-Identity,t-PollRetransmit,pollPDU,pollByte,maxRetxThreshold,t-Reordering,t-StatusProhibit,priority,prioritisedBitRate,bucketSizeDuration,logicalChannelGroup,maxHARQ-Tx,periodicBSR-Timer,retxBSR-Timer,ttiBundling,periodicPHR-Timer,prohibitPHR-Timer,dl-PathlossChange,p-a,betaOffset-ACK-Index,betaOffset-RI-Index,betaOffset-CQI-Index,p0-UE-PUSCH,deltaMCS-Enabled,accumulationEnabled,p0-UE-PUCCH,pSRS-Offset,filterCoefficient,cqi-ReportModeAperiodic,transmissionMode,codebookSubsetRestriction,';
	
	# The count of '@sibX' must be equal with the elements in the @rrc_sib_list!!!
	
	SET @rrc_sib_list = 'sib1,sib2,sib3,sib5,sib6,rrcsetup,';
	SET @param_list = CONCAT_WS(';', @sib1, @sib2, @sib3, @sib5, @sib6, @rrcsetup, '');
	
	SET @t=CONCAT('INSERT INTO table_ueLabels (`ue`, `ue_label`)
			SELECT UE, UE_Label 
			FROM ', @ueLabels, ';');			
	PREPARE stmt FROM @t;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	WHILE (LOCATE(',', @rrc_sib_list) > 0)
		DO
		SET @sib = LEFT(@rrc_sib_list, LOCATE(',',@rrc_sib_list)-1);
		SET @rrc_sib_list = SUBSTRING(@rrc_sib_list, LOCATE(',',@rrc_sib_list) + 1);
		
		SET @params = LEFT(@param_list, LOCATE(';',@param_list)-1);
		SET @param_list = SUBSTRING(@param_list, LOCATE(';',@param_list) + 1);
		
		
		SET @table_headers = '';
		SET @select_params = '';
		
		WHILE (LOCATE(',', @params) > 0)
			DO
			SET @param = LEFT(@params, LOCATE(',',@params)-1);
			SET @params = SUBSTRING(@params, LOCATE(',',@params) + 1);
			
			SET @header = CONCAT('`', @param, '` VARCHAR(15)');
			SET @table_headers = CONCAT_WS(',', @table_headers, @header);
			
			SET @locate_first = CONCAT('LOCATE(\'', @param, '\',`RRC_parsed`)');
			
			IF @param = 'carrierFreq' OR @param = 'dl-CarrierFreq'
				THEN SET @locate_first_carrier = CONCAT('LOCATE(\'', @param, '\\\'\',`RRC_parsed`)'); 
			END IF;
				
			IF @param IN (
			# parameter is TEXT and has ending symbols ',
		/*sib1,2 */		'numberOfRA-Preambles', 'sizeOfRA-PreamblesGroupA', 'messageSizeGroupA', 'powerRampingStep', 'preambleTransMax','ra-ResponseWindowSize', 'alpha', 't300', 't301','t310', 'n310', 't311', 'ul-Bandwidth',
		/*sib5 */		'allowedMeasBandwidth', 'q-OffsetFreq',
		/*rrc_conn_setup */	't-PollRetransmit', 'pollPDU', 'pollByte', 't-Reordering', 'prioritisedBitRate', 'bucketSizeDuration', 'maxHARQ-Tx', 'periodicBSR-Timer', 'retxBSR-Timer', 'periodicPHR-Timer', 'prohibitPHR-Timer', 'deltaMCS-Enabled', 'cqi-ReportModeAperiodic', 'transmissionMode'
					)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+4, LOCATE(\',\',`RRC_parsed`,', @locate_first,')-2-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			ELSEIF @param IN ( 
			# parameter is TEXT and has ending symbols '},
		/*sib1,2 */		'messagePowerOffsetGroupB', 'preambleInitialReceivedTargetPower', 'mac-ContentionResolutionTimer', 'n311',
		/*sib3 */		'q-Hyst',
		/*rrc_conn_setup */	'maxRetxThreshold','t-StatusProhibit', 'dl-PathlossChange', 'p-a', 'filterCoefficient'
						)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_first, ')-2-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			ELSEIF @param IN (
			# parameter has ending symbol ]	
		/*sib1,2 */		'mcc','mnc'
					)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+4, LOCATE(\']\',`RRC_parsed`,', @locate_first, ')-1-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			ELSEIF @param IN (
			# parameter has ending symbol )	
		/*sib3,5 */		'neighCellConfig'
					)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+4, LOCATE(\')\',`RRC_parsed`,', @locate_first, ')-1-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			ELSEIF @param IN (
			# parameter has ending symbol }	
		/*sib3,5 */		'q-QualMin-r9',
		/*sib6 */		'q-QualMin', 't-ReselectionUTRA'
		/*rrc_conn_setup */	
								
					)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_first, ')-1-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			ELSEIF @param IN (
			# parameter is NOT TEXT and has ending symbols },
		/*sib1,2 */		'maxHARQ-Msg3Tx', 'prach-FreqOffset', 'p-b', 'enable64QAM',
		/*sib3 */		's-IntraSearchQ-r9', 's-NonIntraSearchQ-r9', 
		/*rrc_conn_setup */	'ttiBundling', 'logicalChannelGroup', 'betaOffset-CQI-Index'
					)
								
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');

			ELSEIF @param = 'cellReselectionPriority'
				THEN 	IF @sib = 'sib3'
						# RRC is SIB3
						THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
									
					ELSE
						# RRC is not SIB3
						SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
									
					END IF; 
							
			ELSEIF @param = 'q-RxLevMin'
				THEN 	IF @sib = 'sib1'
						# RRC is SIB1
						THEN SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
					ELSE
						# RRC is not SIB1
						SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
										
					END IF; 
					
			ELSEIF @param = 'codebookSubsetRestriction'
							
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+5, LOCATE(\',\',`RRC_parsed`,', @locate_first,')-2-(', @locate_first, '+LENGTH(\'', @param, '\')+4)),15) AS `', @param, '`');				
			
			ELSEIF @param = 'carrierFreq'
							
				THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first_carrier, '+LENGTH(\'', @param, '\\\'\')+2, LOCATE(\',\',`RRC_parsed`,', @locate_first_carrier, ')-(', @locate_first_carrier, '+LENGTH(\'', @param, '\\\'\')+2)),10) AS `', @param, '`');
							
			ELSE 
			# parameter is NOT TEXT and has ending symbol ,
				SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_first, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_first, ')-(', @locate_first, '+LENGTH(\'', @param, '\')+3)),10) AS `', @param, '`');
							
			END IF;

			SET @select_params = CONCAT_WS(',', @select_params, @parameter_location);
			
		END WHILE;
		
		IF @sib = 'sib5' OR @sib = 'sib6'
			THEN 
			
				
				IF @sib = 'sib5'
					THEN SET @params = CONCAT(@sib5,@sib5);
				ELSEIF @sib = 'sib6'
					THEN SET @params = CONCAT(@sib6,@sib6);
				END IF;
				
				SET @i = 1;
				WHILE (LOCATE(',', @params) > 0)
					DO
					SET @param = LEFT(@params, LOCATE(',',@params)-1);
					SET @params = SUBSTRING(@params, LOCATE(',',@params) + 1);
					
					IF (@sib = 'sib5' AND @i<12) OR (@sib = 'sib6' AND @i<9) # locating the second consecutive parameter in the rrc message
						THEN 
							SET @header = CONCAT('`', @param, '_1` VARCHAR(10)');
							SET @table_headers = CONCAT_WS(',', @table_headers, @header);
							
							SET @locate_second = CONCAT('LOCATE(\'', @param, '\',`RRC_parsed`, LOCATE(\'', @param, '\',`RRC_parsed`)+1)');
							
							IF @param = 'carrierFreq' OR @param = 'dl-CarrierFreq'
								THEN SET @locate_second_carrier = CONCAT('LOCATE(\'', @param, '\\\'\',`RRC_parsed`, LOCATE(\'', @param, '\\\'\',`RRC_parsed`)+1)'); 
							END IF;
							
							IF @param IN (
								# parameter is TEXT and has ending symbols ',
							/*sib1,2 */		'numberOfRA-Preambles', 'sizeOfRA-PreamblesGroupA', 'messageSizeGroupA', 'powerRampingStep', 'preambleTransMax','ra-ResponseWindowSize', 'alpha', 't300', 't301','t310', 'n310', 't311', 'ul-Bandwidth',
							/*sib5 */		'allowedMeasBandwidth', 'q-OffsetFreq',
							/*rrc_conn_setup */	't-PollRetransmit', 'pollPDU', 'pollByte', 't-Reordering', 'prioritisedBitRate', 'bucketSizeDuration', 'maxHARQ-Tx', 'periodicBSR-Timer', 'retxBSR-Timer', 'periodicPHR-Timer', 'prohibitPHR-Timer'
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+4, LOCATE(\',\',`RRC_parsed`,', @locate_second,')-2-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN ( 
								# parameter is TEXT and has ending symbols '},
							/*sib1,2 */		'messagePowerOffsetGroupB', 'preambleInitialReceivedTargetPower', 'mac-ContentionResolutionTimer', 'n311',
							/*sib3 */		'q-Hyst',
							/*rrc_conn_setup */	'maxRetxThreshold','t-StatusProhibit', 'dl-PathlossChange', 'p-a'
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_second, ')-2-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN (
								# parameter has ending symbol ]	
							/*sib1,2 */		'mcc','mnc'
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+4, LOCATE(\']\',`RRC_parsed`,', @locate_second, ')-1-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN (
								# parameter has ending symbol )	
							/*sib3,5 */		'neighCellConfig'
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+4, LOCATE(\')\',`RRC_parsed`,', @locate_second, ')-1-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN (
								# parameter has ending symbol }	
							/*sib3,5 */		'q-QualMin-r9',
							/*sib6 */		'q-QualMin', 't-ReselectionUTRA'
							/*rrc_conn_setup */	
								
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_second, ')-1-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN (
								# parameter is NOT TEXT and has ending symbols },
							/*sib1,2 */		'maxHARQ-Msg3Tx', 'prach-FreqOffset', 'p-b', 'enable64QAM',
							/*sib3 */		's-IntraSearchQ-r9', 's-NonIntraSearchQ-r9', 
							/*rrc_conn_setup */	'ttiBundling', 'logicalChannelGroup'
									)
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');

							ELSEIF @param = 'cellReselectionPriority'
								THEN 	IF @sib = 'sib3'
										# RRC is SIB3
										THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
									
									ELSE
										# RRC is not SIB3
										SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
									
									END IF; 
							
							ELSEIF @param = 'q-RxLevMin'
								THEN 	IF @sib = 'sib1'
										# RRC is SIB1
										THEN SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
									ELSE
										# RRC is not SIB1
										SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
										
									END IF; 
							
							ELSEIF @param = 'carrierFreq'
							
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second_carrier, '+LENGTH(\'', @param, '\\\'\')+2, LOCATE(\',\',`RRC_parsed`,', @locate_second_carrier, ')-(', @locate_second_carrier, '+LENGTH(\'', @param, '\\\'\')+2)),10) END AS `', @param, '`');
							
							ELSE 
								# parameter is NOT TEXT and has ending symbol ,
									SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_second, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_second, ')-(', @locate_second, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							END IF;
					
							SET @select_params = CONCAT(@select_params, ', CASE WHEN ',@locate_second_carrier,' <> 0 THEN ', @parameter_location);
							
							SET @i = @i+1;	
								
						
						ELSE	# locating the third consecutive parameter in the rrc message							

							SET @header = CONCAT('`', @param, '_2` VARCHAR(10)');
							SET @table_headers = CONCAT_WS(',', @table_headers, @header);
								
							SET @locate_third = CONCAT('LOCATE(\'', @param, '\',`RRC_parsed`, LOCATE(\'', @param, '\',`RRC_parsed`, LOCATE(\'', @param, '\',`RRC_parsed`)+1)+1)');
							
							IF @param = 'carrierFreq' OR @param = 'dl-CarrierFreq'
								THEN SET @locate_third_carrier = CONCAT('LOCATE(\'', @param, '\\\'\',`RRC_parsed`, LOCATE(\'', @param, '\\\'\',`RRC_parsed`,LOCATE(\'', @param, '\\\'\',`RRC_parsed`)+1)+1)'); 
							END IF;
							
							IF @param IN (
								# parameter is TEXT and has ending symbols ',
							/*sib1,2 */		'numberOfRA-Preambles', 'sizeOfRA-PreamblesGroupA', 'messageSizeGroupA', 'powerRampingStep', 'preambleTransMax','ra-ResponseWindowSize', 'alpha', 't300', 't301','t310', 'n310', 't311', 'ul-Bandwidth',
							/*sib5 */		'allowedMeasBandwidth', 'q-OffsetFreq',
							/*rrc_conn_setup */	't-PollRetransmit', 'pollPDU', 'pollByte', 't-Reordering', 'prioritisedBitRate', 'bucketSizeDuration', 'maxHARQ-Tx', 'periodicBSR-Timer', 'retxBSR-Timer', 'periodicPHR-Timer', 'prohibitPHR-Timer'
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+4, LOCATE(\',\',`RRC_parsed`,', @locate_third,')-2-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
								
							ELSEIF @param IN ( 
								# parameter is TEXT and has ending symbols '},
							/*sib1,2 */		'messagePowerOffsetGroupB', 'preambleInitialReceivedTargetPower', 'mac-ContentionResolutionTimer', 'n311',
							/*sib3 */		'q-Hyst',
							/*rrc_conn_setup */	'maxRetxThreshold','t-StatusProhibit', 'dl-PathlossChange', 'p-a'
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_third, ')-2-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
								
							ELSEIF @param IN (
								# parameter has ending symbol ]	
							/*sib1,2 */		'mcc','mnc'
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+4, LOCATE(\']\',`RRC_parsed`,', @locate_third, ')-1-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
								
							ELSEIF @param IN (
								# parameter has ending symbol )	
							/*sib3,5 */		'neighCellConfig'
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+4, LOCATE(\')\',`RRC_parsed`,', @locate_third, ')-1-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
							
							ELSEIF @param IN (
								# parameter has ending symbol }	
							/*sib3,5 */		'q-QualMin-r9',
							/*sib6 */		'q-QualMin', 't-ReselectionUTRA'
							/*rrc_conn_setup */	
									
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+4, LOCATE(\'}\',`RRC_parsed`,', @locate_third, ')-1-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
								
							ELSEIF @param IN (
								# parameter is NOT TEXT and has ending symbols },
							/*sib1,2 */		'maxHARQ-Msg3Tx', 'prach-FreqOffset', 'p-b', 'enable64QAM',
							/*sib3 */		's-IntraSearchQ-r9', 's-NonIntraSearchQ-r9', 
							/*rrc_conn_setup */	'ttiBundling', 'logicalChannelGroup'
									)
									
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');

							ELSEIF @param = 'cellReselectionPriority'
								THEN 	IF @sib = 'sib3'
										# RRC is SIB3
										THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
										
									ELSE
										# RRC is not SIB3
										SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
										
									END IF; 
								
							ELSEIF @param = 'q-RxLevMin'
								THEN 	IF @sib = 'sib1'
										# RRC is SIB1
										THEN SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\'}\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
									ELSE
										# RRC is not SIB1
										SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
											
									END IF; 
							
							ELSEIF @param = 'carrierFreq'
								
								THEN 	SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third_carrier, '+LENGTH(\'', @param, '\\\'\')+2, LOCATE(\',\',`RRC_parsed`,', @locate_third_carrier, ')-(', @locate_third_carrier, '+LENGTH(\'', @param, '\\\'\')+2)),10) END AS `', @param, '`');
								
							ELSE 
								# parameter is NOT TEXT and has ending symbol ,
									SET @parameter_location = CONCAT('LEFT(MID(`RRC_parsed`, ', @locate_third, '+LENGTH(\'', @param, '\')+3, LOCATE(\',\',`RRC_parsed`,', @locate_third, ')-(', @locate_third, '+LENGTH(\'', @param, '\')+3)),10) END AS `', @param, '`');
								
							END IF;
									
						SET @select_params = CONCAT(@select_params, ', CASE WHEN ', @locate_second_carrier,' <> 0  AND ', @locate_third_carrier,' <> 0 THEN ', @parameter_location);
						SET @i = @i+1;	
					END IF;
				END WHILE;					
		END IF;
				
		
		SET @table_benchParams = CONCAT(ue_table,'_bench_',@sib);
		
		SET @t=CONCAT('DROP TABLE IF EXISTS ',@table_benchParams);
		
		PREPARE stmt FROM @t;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
			
		SET @t=CONCAT('
			CREATE TABLE ',@table_benchParams,' (
			     ue varchar(10), 
			     ue_label TEXT, 
			     Channel varchar(10), 
			     RRC_Message_Type TEXT',
			     @table_headers,')'
			     );
		
		PREPARE stmt FROM @t;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @j = 1;	
		SET @i = (SELECT COUNT(*) FROM table_ueLabels);
		WHILE @j<=@i 
			DO
			SET @ue = (
				SELECT ue
				FROM table_ueLabels
				WHERE id = @j);
			
			SET @ue_label = (
				SELECT ue_label
				FROM table_ueLabels
				WHERE id = @j);
			
			IF @sib = 'sib1' 
				THEN SET @sib_finder = 'SystemInformationBlockType1';
			
			ELSEIF @sib = 'rrcsetup'
				THEN SET @sib_finder = 'RRCConnectionSetup';
			
			ELSE SET @sib_finder = UPPER(@sib);
			
			END IF;
			
			SET @c=CONCAT('SET @chan = (SELECT `COLUMN_NAME`
				FROM  `information_schema`.`COLUMNS`
				WHERE `TABLE_NAME` = \'', ue_table, '_', @ue, '\' AND `COLUMN_NAME` = \'Channel\')');
			
			PREPARE stmt FROM @c;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			IF @chan IS NOT NULL
				THEN	SET @p = CONCAT('INSERT INTO ',@table_benchParams, 
								' SELECT DISTINCT \'',@ue,'\' as UE,\'',@ue_label, '\' as UE_Label, Channel, RRC_Msg ', @select_params, 
								' FROM ', ue_table, '_', @ue,
								' WHERE RRC_Msg <> \'\' AND LOCATE(\'',@sib_finder,'\',RRC_Msg) <> 0 AND LOCATE(\'Complete\', RRC_Msg) = 0'
							);
				ELSE 	SET @p = CONCAT('INSERT INTO ',@table_benchParams, 
								' SELECT DISTINCT \'',@ue,'\' as UE,\'',@ue_label, '\' as UE_Label, \'n/a\' AS Channel, RRC_Msg ', @select_params, 
								' FROM ', ue_table, '_', @ue,
								' WHERE RRC_Msg <> \'\' AND LOCATE(\'',@sib_finder,'\',RRC_Msg) <> 0 AND LOCATE(\'Complete\', RRC_Msg) = 0'
							);
			END IF;
			
			IF @sib = 'sib6' THEN SET @f = @p; END IF;
			
			PREPARE stmt FROM @p;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;

			
			SET @j = @j + 1;
		END WHILE;
	END WHILE;
END IF;

END$$

DELIMITER ;	