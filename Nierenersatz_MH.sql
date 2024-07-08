-- runs well on D, except ECMO one more Patient having a RangeSignals (2024) that would be corrected and which is not on D; could run full ECMO on P
-- SOLUTION: connect to [DWH-SQL-P]; since within the query it refers to [DWH-SQL-D] when needed (for the Signals)

WITH CTE_RRT_raw AS ( -- Raw Data of Renal Replacement Therapy (RangeSignals & TextSignals & Signals)
	-- RangeSignals
	SELECT 
		RS.patientid AS PID_PDMS,
		PAT.hospitalnumber AS PID_KISPI,
		PAT.socialsecurity AS FID_KISPI,
		RS.parameterid AS ParameterID,
		P.parametername AS ParameterName,
		NULL AS Text,
		RS.starttime AS StartTime,
		RS.endtime AS EndTime,
		(DATEDIFF(MINUTE, RS.starttime, RS.endtime)*RS.value)/U.multiplier AS Value, -- RS.value = amount per minute -> calculate for entire duration; and Value needs to be divided by Multiplier to match indicated unit
		U.unitname AS Unit,
		RS.status AS Status
	FROM [CDMH_PSA].mv5_out.psa_rangesignals_cur RS
	LEFT JOIN [CDMH_PSA].mv5_out.psa_parameters_mv5_cur P ON RS.parameterid = P.parameterid
	LEFT JOIN [CDMH_PSA].mv5_out.psa_units_mv5_cur U ON P.unitid = U.unitid -- for Multiplicator and Unit
	LEFT JOIN [CDMH_PSA].mv5_out.psa_patients_cur PAT ON RS.patientid = PAT.patientid
	WHERE RS.patientid IN (
		156215, 174625, -- ECMO: Patient1 == KISPI_FID: 2422528, KISPI_PID: 5129905 / Patient2 == KISPI_FID: 2689144, KISPI_PID: 5571524
		159147, -- Prismax: Patient1 == KISPI_FID: 2398502, KISPI_PID: 3975824
		156600, -- Prismaflex: Patient1 == KISPI_FID: 2448710, KISPI_PID: 5405804
		156215, 156607 -- Peritonealdialyse: Patient1 == KISPI_FID: 2422528, KISPI_PID: 5129905 / Patient2 == KISPI_FID: 2440438, KISPI_PID: 5400560
	) 
	AND RS.parameterid IN (
		8554, 8007, -- ECMO: Hauptparameter [8554, ECMO Hämofiltration ml/h], Nebenparameter [8007, Hemosol B0_]
		23920, -- Prismax: Nebenparameter [23920, Prismax läuft]
		4063, -- Prismaflex: Nebenparameter [4063, Hämofiltration Start] 
		4064 -- Peritonealdialyse: Hauptparameter [4064, Peritonealdialyse] 
	)
	AND RS.status IN (0,1,2)

	UNION
	-- TextSignals
	SELECT 
		TS.patientid AS PID_PDMS,
		PAT.hospitalnumber AS PID_KISPI,
		PAT.socialsecurity AS FID_KISPI,
		TS.parameterid AS ParameterID,
		P.parametername AS ParameterName,
		TEXT.text AS Text,
		TS.time AS StartTime,
		DATEADD(MINUTE, 1, TS.time) AS EndTime,
		NULL AS Value,
		NULL AS Unit,
		NULL AS Status
	FROM [CDMH_PSA].mv5_out.psa_textsignals_cur TS
	LEFT JOIN [CDMH_PSA].mv5_out.psa_parameters_mv5_cur P ON TS.parameterid = P.parameterid
	LEFT JOIN [CDMH_PSA].mv5_out.psa_units_mv5_cur U ON P.unitid = U.unitid -- for Multiplicator and Unit
	LEFT JOIN [CDMH_PSA].mv5_out.psa_patients_cur PAT ON TS.patientid = PAT.patientid
	LEFT JOIN [CDMH_PSA].mv5_out.psa_parameterstext_cur TEXT ON TS.parameterid = TEXT.parameterid AND TS.textid = TEXT.textid
	WHERE TS.patientid IN (
		159147 -- Prismax: Patient1 == KISPI_FID: 2398502, KISPI_PID: 3975824
	) 
	AND TS.parameterid IN (
		15120 -- Prismax: Hauptparameter [15120, HF-Modus (CRRT) (set)]
	)

	UNION

	-- Signals
	SELECT 
		S.patientid AS PID_PDMS,
		PAT.hospitalnumber AS PID_KISPI,
		PAT.socialsecurity AS FID_KISPI,
		S.parameterid AS ParameterID,
		P.parametername AS ParameterName,
		NULL AS Text,
		S.time AS StartTime,
		DATEADD(MINUTE, 1, S.time) AS EndTime,
		S.value AS Value,
		U.unitname AS Unit,
		NULL AS Status
	--FROM [CDMH_PSA].mv5_out.psa_signals_cur S -- not complete Signals on [DWH-SQL-P] so far (date: 20240522); therefore take Signals from [DWH-SQL-D]
	FROM [dwh-sql-d].[CDMH_PSA].[mv5].[psa_Signals] S
	LEFT JOIN [CDMH_PSA].mv5_out.psa_parameters_mv5_cur P ON S.parameterid = P.parameterid
	LEFT JOIN [CDMH_PSA].mv5_out.psa_units_mv5_cur U ON P.unitid = U.unitid -- for Multiplicator and Unit
	LEFT JOIN [CDMH_PSA].mv5_out.psa_patients_cur PAT ON S.patientid = PAT.patientid
	WHERE S.patientid IN (
		156600, -- Prismaflex: Patient1 == KISPI_FID: 2448710, KISPI_PID: 5405804
		156215, 156607 -- Peritonealdialyse: Patient1 == KISPI_FID: 2422528, KISPI_PID: 5129905 / Patient2 == KISPI_FID: 2440438, KISPI_PID: 5400560
	) 
	AND S.parameterid IN (
		12, -- Prismaflex: Hauptparameter [12, Hämofiltration Zugangsdruck]
		1765 -- Peritonealdialyse: Nebenparameter [1765, Peritoneladialyse Entleerung]
	)
),
CTE_RRT_P1P2 AS ( -- Define <ParameterID> = Parameter P1, <ParameterID> = Parameter P2
	SELECT 
		PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		CASE
			WHEN ParameterID IN (8554, 8007) THEN 'Hämofiltration' --ECMO
			WHEN ParameterID IN (15120, 23920) THEN 'Hämodiafiltration (Prismax)' --Prismax
			WHEN ParameterID IN (12, 4063) THEN 'Hämodiafiltration (Prismaflex)' --Prismaflex
			WHEN ParameterID IN (4064, 1765) THEN 'Peritonealdialyse' --PD
		END AS RRT,
		CASE
			WHEN ParameterID IN (8554, 15120, 12, 4064) THEN 'P1'
			WHEN ParameterID IN (8007, 23920, 4063, 1765) THEN 'P2'
		END AS Parameter,
		StartTime, 
		EndTime,
		DATEDIFF(MINUTE, StartTime, EndTime) AS Duration_min
	FROM CTE_RRT_raw
),
CTE_RRT_MainSide AS ( -- Define Parameter P1 = Main Parameter, Parameter P2 = Side Parameter (if Main Parameter present meanwhile, else will be Main Parameter itself)
	SELECT
		P1P2.PID_PDMS,
		P1P2.PID_KISPI,
		P1P2.FID_KISPI,
		P1P2.RRT,
		P1P2.Parameter,
		CASE	
			WHEN P1P2.Parameter = 'P1' THEN 'Main'
			WHEN P1P2.Parameter = 'P2' AND EXISTS (
				SELECT 1
				FROM CTE_RRT_P1P2 p1p2
				WHERE (p1p2.Parameter = 'P1'
					AND p1p2.RRT = P1P2.RRT
					AND (
						(P1P2.StartTime <= p1p2.StartTime AND P1P2.EndTime >= p1p2.StartTime) OR -- Side Parameter starts before Main Parameter, and Side Parameter ends after Start of Main Parameter
						(P1P2.StartTime <= p1p2.EndTime AND P1P2.EndTime >= p1p2.EndTime) OR -- Side Parameter starts before End of Main Parameter, and Side Parameter ends after End of Main Parameter
						(P1P2.StartTime >= p1p2.StartTime AND P1P2.EndTime <= p1p2.EndTime) OR -- Side Parameter starts after Main Parameter, and Side Parameter ends before End of Main Parameter
						-- two special cases if Start and End of Side Parameter within 15min-gap range; meaning not overlapping with Main Parameter
						(P1P2.StartTime >= DATEADD(MINUTE, -15, p1p2.StartTime) AND P1P2.EndTime  < p1p2.StartTime) OR -- Side Parameter starts within gap range of 15min before Main Parameter, and Side Parameter ends before Start of Main Parameter
						(P1P2.EndTime <= DATEADD(MINUTE, 15, p1p2.EndTime) AND P1P2.StartTime > p1p2.EndTime) -- Side Parameter ends within gap range of 15min after Main Parameter, and Side Parameter starts after End of Main Parameter
					)
				)
				AND p1p2.PID_PDMS = P1P2.PID_PDMS
			) THEN 'Side'
			ELSE 'Main'
		END AS ParameterType,
		P1P2.StartTime,
		P1P2.EndTime,
		P1P2.Duration_min
	FROM CTE_RRT_P1P2 P1P2
),

CTE_RRT_TimeGaps AS ( 
	-- new columns "PrevEndTime" & "NextStartTime"
	-- NOTE: sees Start of the first Main Parameter Batches (PrevEndTime = NULL) and End of the last Main Parameter Batch (NextStartTime = NULL); Side Parameter RangeSignals will always be NULL
	-- NOTE: gaps are not visible yet
    SELECT 
        *,		
		CASE
			WHEN ParameterType = 'Main' THEN LAG(EndTime) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, RRT, ParameterType ORDER BY StartTime) 
			ELSE NULL
		END AS PrevEndTime,
		CASE
			WHEN ParameterType = 'Main' THEN LEAD(StartTime) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, RRT, ParameterType ORDER BY StartTime) 
			ELSE NULL
		END AS NextStartTime
    FROM 
        CTE_RRT_MainSide
),
CTE_RRT_TimeSpans AS ( 
	-- new columns "TimeSpanBefore" & "TimeSpanAfter" ca= "PrevEndTime" & "NextStartTime", but additionally consider gaps, but ignores gaps up to 15min and, respectively, assignes 0 = vs. 1
	-- new column "BatchID" indicating the timespan-number for Main Parameter RangeSignals, NULL for Side Parameter RangeSignals
    SELECT 
        *,
        CASE 
            WHEN (ParameterType = 'Main') AND (StartTime <= DATEADD(MINUTE, 15, PrevEndTime)) THEN 1 -- ignore gap up to 15min 
			WHEN (ParameterType = 'Main') AND (StartTime > DATEADD(MINUTE, 15, PrevEndTime)) THEN 0
            ELSE NULL
        END AS TimeSpanBefore,
        CASE 
            WHEN (ParameterType = 'Main') AND (EndTime >= DATEADD(MINUTE, -15, NextStartTime)) THEN 1 -- ignore gap up to 15min
			WHEN (ParameterType = 'Main') AND (EndTime < DATEADD(MINUTE, -15, NextStartTime)) THEN 0
            ELSE NULL
        END AS TimeSpanAfter,
		CASE
			WHEN ParameterType = 'Main'
				THEN SUM(CASE WHEN StartTime > DATEADD(MINUTE, 15, PrevEndTime) THEN 1 ELSE 0 END) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, RRT ORDER BY StartTime)  -- i.e. new batch when StartTime > PrevEndTime (+15min), else same batch
		END AS BatchID
    FROM 
        CTE_RRT_TimeGaps
),
CTE_RRT_MergedBatches AS ( -- group by patient/ Main Parameter Batch of Main Parameter RangeSignals UNION Side Parameters RangeSignals
    SELECT 
        PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		RRT,
        Parameter,
        ParameterType,
        MIN(StartTime) AS StartTime,
        MAX(EndTime) AS EndTime,
        SUM(Duration_min) AS Duration_min,
        BatchID
    FROM 
        CTE_RRT_TimeSpans
	WHERE BatchID IS NOT NULL -- only Main Parameter RangeSignals, which constitute Main Parameter Batches
    GROUP BY 
        PID_PDMS, PID_KISPI, FID_KISPI, RRT, Parameter, ParameterType, BatchID

	UNION

	SELECT 
		PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		RRT,
		Parameter,
		ParameterType,
		StartTime,
		EndTime,
		Duration_min,
		BatchID
    FROM 
        CTE_RRT_TimeSpans
	WHERE BatchID IS NULL -- only Side Parameter RangeSignals, which will be used to correct Main Parameter Batches
),
CTE_RRT_PotentialCorrections AS ( -- just to check which Side Parameter RangeSignals would be used to correct a Main Parameter Batch (CTE won't be used further)
	SELECT
	*,
	CASE
		WHEN T1.BatchID IS NOT NULL THEN NULL
		WHEN T1.BatchID IS NULL AND EXISTS(
			SELECT 1
			FROM CTE_RRT_MergedBatches T2
			WHERE T2.PID_PDMS = T1.PID_PDMS
			AND T2.PID_KISPI = T1.PID_KISPI
			AND T2.FID_KISPI = T1.FID_KISPI
			AND T2.RRT = T1.RRT
			AND T2.BatchID IS NOT NULL
			AND T1.StartTime < T2.StartTime -- Start of Side Parameter before Start of Main Parameter
			AND T1.StartTime >= DATEADD(MINUTE, -60, T2.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter
			AND T1.EndTime > DATEADD(MINUTE, -15, T2.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 15min
		) THEN 'StartTime'
		WHEN T1.BatchID IS NULL AND EXISTS(
			SELECT 1
			FROM CTE_RRT_MergedBatches T2
			WHERE T2.PID_PDMS = T1.PID_PDMS
			AND T2.PID_KISPI = T1.PID_KISPI
			AND T2.FID_KISPI = T1.FID_KISPI
			AND T2.RRT = T1.RRT
			AND T2.BatchID IS NOT NULL
			AND T1.EndTime > T2.EndTime -- End of Side Parameter after End of Main Parameter
			AND T1.EndTime <= DATEADD(MINUTE, 60, T2.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
			AND T1.StartTime < DATEADD(MINUTE, 15, T2.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 15min
		) THEN 'EndTime'
		END AS UsedToCorrect
	FROM CTE_RRT_MergedBatches T1
),
CTE_RRT_Correction AS ( -- correct Main Parameter Batches with Side Parameter RangeSignals if present
	SELECT
		M.PID_PDMS,
		M.PID_KISPI,
		M.FID_KISPI,
		M.RRT,
		COALESCE( --if an earlier overlapping Side Parameter RangeSignal present: take this
			(SELECT TOP 1 S.StartTime
			FROM CTE_RRT_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.RRT = M.RRT
			AND S.ParameterType = 'Side'
			AND S.StartTime < M.StartTime -- Start of Side Parameter before Start of Main Parameter
			AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter
			AND S.EndTime > DATEADD(MINUTE, -15, M.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 15min
			ORDER BY S.StartTime ASC), M.StartTime
		) AS StartTime,
		COALESCE( --if a later overlapping Side Parameter RangeSignal present: take this
			(SELECT TOP 1 S.EndTime
			FROM CTE_RRT_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.RRT = M.RRT
			AND S.ParameterType = 'Side'
			AND S.EndTime > M.EndTime -- End of Side Parameter after End of Main Parameter
			AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
			AND S.StartTime < DATEADD(MINUTE, 15, M.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 15min
			ORDER BY S.EndTime DESC), M.EndTime
		) AS EndTime,
		DATEDIFF(MINUTE,
			COALESCE( -- whatever will be the StartTime of a Batch (corrected or not)
				(SELECT TOP 1 S.StartTime
				FROM CTE_RRT_MergedBatches S
				WHERE S.PID_PDMS = M.PID_PDMS
				AND S.PID_KISPI = M.PID_KISPI
				AND S.FID_KISPI = M.FID_KISPI
				AND S.RRT = M.RRT
				AND S.ParameterType = 'Side'
				AND S.StartTime < M.StartTime -- Start of Side Parameter before Start of Main Parameter
				AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter
				AND S.EndTime > DATEADD(MINUTE, -15, M.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 15min
				ORDER BY S.StartTime ASC), M.StartTime),
			COALESCE( -- whatever will be the EndTime of a Batch (corrected or not)
				(SELECT TOP 1 S.EndTime
				FROM CTE_RRT_MergedBatches S
				WHERE S.PID_PDMS = M.PID_PDMS
				AND S.PID_KISPI = M.PID_KISPI
				AND S.FID_KISPI = M.FID_KISPI
				AND S.RRT = M.RRT
				AND S.EndTime > M.EndTime -- End of Side Parameter after End of Main Parameter
				AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
				AND S.StartTime < DATEADD(MINUTE, 15, M.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 15min
				ORDER BY S.EndTime DESC), M.EndTime)
		) AS Duration_min -- no better way to calculate duration in the same CTE (alternative: new CTE)
			
	FROM CTE_RRT_MergedBatches M
	WHERE M.ParameterType = 'Main'
)

SELECT *
FROM CTE_RRT_Correction
--WHERE RRT = 'PD'
ORDER BY StartTime





/*
#############################################################################################
Not working alternatives for correction; maybe as Plan B if Plan A takes too long
CTE_ECMO_Correction_Opt1 AS (
	SELECT
		M.PID_PDMS,
		M.PID_KISPI,
		M.FID_KISPI,
		'ECMO' AS Nierenersatz,
		COALESCE(MIN(CASE WHEN S.ParameterType = 'ECMO_Side' 
							AND S.StartTime < M.StartTime
							AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) 
							AND S.EndTime > M.StartTime
						THEN S.StartTime END), M.StartTime) AS StartTime,
		COALESCE(MAX(CASE WHEN S.ParameterType = 'ECMO_Side' 
							AND S.StartTime < M.EndTime
							AND S.EndTime > M.EndTime
							AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime)
						THEN S.EndTime END), M.EndTime) AS EndTime,
		 DATEDIFF(MINUTE, 
				 COALESCE(MIN(CASE WHEN S.ParameterType = 'ECMO_Side' 
										AND S.StartTime < M.StartTime
										AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) 
										AND S.EndTime > M.StartTime
								   THEN S.StartTime END), M.StartTime), 
				 COALESCE(MAX(CASE WHEN S.ParameterType = 'ECMO_Side' 
										AND S.StartTime < M.EndTime
										AND S.EndTime > M.EndTime
										AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime)
								   THEN S.EndTime END), M.EndTime)
            ) AS Duration_min
	FROM 
		CTE_ECMO_MergedBatches M
	LEFT JOIN CTE_ECMO_MergedBatches S ON M.PID_PDMS = S.PID_PDMS AND M.PID_KISPI = S.PID_KISPI AND M.FID_KISPI = S.FID_KISPI
	GROUP BY 
		M.PID_PDMS, M.PID_KISPI, M.FID_KISPI, M.StartTime, M.EndTime

),
CTE_ECMO_Correction_Opt2 AS (
	SELECT
		M.PID_PDMS,
		M.PID_KISPI,
		M.FID_KISPI,
		'ECMO' AS Nierenersatz,
		COALESCE(
			(SELECT MIN(S.StartTime)
			FROM CTE_ECMO_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.ParameterType = 'ECMO_Side'
			AND S.StartTime < M.StartTime
			AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) 
			AND S.EndTime > M.StartTime), M.StartTime
		) AS StartTime,
		COALESCE(
			(SELECT MAX(S.EndTime)
			FROM CTE_ECMO_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.ParameterType = 'ECMO_Side'
			AND S.StartTime < M.EndTime
			AND S.EndTime > M.EndTime
			AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime)), M.EndTime
		) AS EndTime,
		DATEDIFF(MINUTE, 
				 COALESCE(
					(SELECT MIN(S.StartTime)
					FROM CTE_ECMO_MergedBatches S
					WHERE S.PID_PDMS = M.PID_PDMS
					AND S.PID_KISPI = M.PID_KISPI
					AND S.FID_KISPI = M.FID_KISPI
					AND S.ParameterType = 'ECMO_Side'
					AND S.StartTime < M.StartTime
					AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) 
					AND S.EndTime > M.StartTime), M.StartTime
				), 
				COALESCE(
					(SELECT MAX(S.EndTime)
					FROM CTE_ECMO_MergedBatches S
					WHERE S.PID_PDMS = M.PID_PDMS
					AND S.PID_KISPI = M.PID_KISPI
					AND S.FID_KISPI = M.FID_KISPI
					AND S.ParameterType = 'ECMO_Side'
					AND S.StartTime < M.EndTime
					AND S.EndTime > M.EndTime
					AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime)), M.EndTime
				)
            ) AS Duration_min
	FROM 
		CTE_ECMO_MergedBatches M
	LEFT JOIN CTE_ECMO_MergedBatches S ON M.PID_PDMS = S.PID_PDMS AND M.PID_KISPI = S.PID_KISPI AND M.FID_KISPI = S.FID_KISPI
	GROUP BY 
		M.PID_PDMS, M.PID_KISPI, M.FID_KISPI, M.StartTime, M.EndTime

)
#############################################################################################
*/