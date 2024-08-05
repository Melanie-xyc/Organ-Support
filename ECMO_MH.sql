WITH CTE_ECMO_raw AS ( -- Raw Data of ECMO (RangeSignals & TextSignals & Signals)
	-- RangeSignals
	SELECT 
		RS.patientid AS PID_PDMS,
		PAT.hospitalnumber AS PID_KISPI,
		PAT.socialsecurity AS FID_KISPI,
		RS.parameterid AS ParameterID,
		P.parametername AS ParameterName,
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
		156215, 164403, 164595  -- ECMO: Patient1 == KISPI_FID: 2422528, KISPI_PID: 5129905 / Patient2 == KISPI_FID: 2552845, KISPI_PID: 5480945 / Patient3 == KISPI_FID: 2519490, KISPI_PID: 5275209
	) 
	AND RS.parameterid IN (
		4067 -- ECMO: Nebenparameter [4067, ECMO Start]
	)
	AND RS.status <> 4

	UNION

	-- Signals
	SELECT 
		S.patientid AS PID_PDMS,
		PAT.hospitalnumber AS PID_KISPI,
		PAT.socialsecurity AS FID_KISPI,
		S.parameterid AS ParameterID,
		P.parametername AS ParameterName,
		S.time AS StartTime,
		DATEADD(MINUTE, 1, S.time) AS EndTime,
		S.value AS Value,
		U.unitname AS Unit,
		NULL AS Status
	-- not complete Signals on [DWH-SQL-P] so far (date: 20240522); therefore take Signals from [DWH-SQL-D]
	FROM [dwh-sql-d].[CDMH_PSA].[mv5].[psa_Signals] S
	LEFT JOIN [CDMH_PSA].mv5_out.psa_parameters_mv5_cur P ON S.parameterid = P.parameterid
	LEFT JOIN [CDMH_PSA].mv5_out.psa_units_mv5_cur U ON P.unitid = U.unitid -- for Multiplicator and Unit
	LEFT JOIN [CDMH_PSA].mv5_out.psa_patients_cur PAT ON S.patientid = PAT.patientid
	WHERE S.patientid IN (
		156215, 164403, 164595 -- ECMO: Patient1 == KISPI_FID: 2422528, KISPI_PID: 5129905 / Patient2 == KISPI_FID: 2689144, KISPI_PID: 5571524 / Patient3 == KISPI_FID: 2731236, KISPI_PID: 5419368
	) 
	AND S.parameterid IN (
		27981 -- ECMO: Hauptparameter [27981, Generic Pump Rotation Speed  (Xenios) (set)]  
	)
),
CTE_ECMO_P1P2 AS ( -- Define <ParameterID> = Parameter P1, <ParameterID> = Parameter P2
	SELECT 
		PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		'ECMO' AS ECMO,
		ParameterID,
		CASE
			WHEN ParameterID = 27981 THEN 'P1'
			WHEN ParameterID = 4067 THEN 'P2'
		END AS Parameter,
		StartTime, 
		EndTime,
		DATEDIFF(MINUTE, StartTime, EndTime) AS Duration_min
	FROM CTE_ECMO_raw
),
CTE_ECMO_MainSide AS ( -- Define Parameter P1 = Main Parameter, Parameter P2 = Side Parameter (if Main Parameter present meanwhile, else will be Main Parameter itself)
	SELECT
		T1.PID_PDMS,
		T1.PID_KISPI,
		T1.FID_KISPI,
		T1.ECMO,
		T1.Parameter,
		CASE	
			WHEN T1.Parameter = 'P1' THEN 'Main'
			WHEN T1.Parameter = 'P2' AND EXISTS (
				SELECT 1
				FROM CTE_ECMO_P1P2 T2
				WHERE T2.Parameter = 'P1'
					AND T2.ECMO = T1.ECMO
					AND T2.PID_PDMS = T1.PID_PDMS
					AND (
						(T1.StartTime <= T2.StartTime AND T1.EndTime >= T2.StartTime) OR -- Side Parameter starts before Main Parameter, and Side Parameter ends after Start of Main Parameter
						(T1.StartTime <= T2.EndTime AND T1.EndTime >= T2.EndTime) OR -- Side Parameter starts before End of Main Parameter, and Side Parameter ends after End of Main Parameter
						(T1.StartTime >= T2.StartTime AND T1.EndTime <= T2.EndTime) OR -- Side Parameter starts after Main Parameter, and Side Parameter ends before End of Main Parameter
						-- two special cases if Start and End of Side Parameter within 12h-gap range; meaning not overlapping with Main Parameter
						(T1.StartTime >= DATEADD(HOUR, -12, T2.StartTime) AND T1.EndTime  < T2.StartTime) OR -- Side Parameter starts within gap range of 12h before Main Parameter, and Side Parameter ends before Start of Main Parameter
						(T1.EndTime <= DATEADD(HOUR, 12, T2.EndTime) AND T1.StartTime > T2.EndTime) -- Side Parameter ends within gap range of 12h after Main Parameter, and Side Parameter starts after End of Main Parameter
					)
			) THEN 'Side'
			ELSE 'Main'
		END AS ParameterType,
		T1.StartTime,
		T1.EndTime,
		T1.Duration_min
	FROM CTE_ECMO_P1P2 T1
),

CTE_ECMO_TimeGaps AS ( 
	-- new columns "PrevEndTime" & "NextStartTime"
	-- NOTE: sees Start of the first Main Parameter Batches (PrevEndTime = NULL) and End of the last Main Parameter Batch (NextStartTime = NULL); Side Parameter RangeSignals will always be NULL
	-- NOTE: gaps are not visible yet
    SELECT 
        *,		
		CASE
			WHEN ParameterType = 'Main' THEN LAG(EndTime) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, ECMO, ParameterType ORDER BY StartTime) 
			ELSE NULL
		END AS PrevEndTime,
		CASE
			WHEN ParameterType = 'Main' THEN LEAD(StartTime) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, ECMO, ParameterType ORDER BY StartTime) 
			ELSE NULL
		END AS NextStartTime
    FROM 
        CTE_ECMO_MainSide
),
CTE_ECMO_TimeSpans AS ( 
	-- new columns "TimeSpanBefore" & "TimeSpanAfter" ca= "PrevEndTime" & "NextStartTime", but additionally consider gaps, but ignores gaps up to 12h and, respectively, assignes 0 = vs. 1
	-- new column "BatchID" indicating the timespan-number for Main Parameter RangeSignals, NULL for Side Parameter RangeSignals
    SELECT 
        *,
        CASE 
            WHEN (ParameterType = 'Main') AND (StartTime <= DATEADD(HOUR, 12, PrevEndTime)) THEN 1 -- ignore gap up to 12h 
			WHEN (ParameterType = 'Main') AND (StartTime > DATEADD(HOUR, 12, PrevEndTime)) THEN 0
            ELSE NULL
        END AS TimeSpanBefore,
        CASE 
            WHEN (ParameterType = 'Main') AND (EndTime >= DATEADD(HOUR, -12, NextStartTime)) THEN 1 -- ignore gap up to 12h
			WHEN (ParameterType = 'Main') AND (EndTime < DATEADD(HOUR, -12, NextStartTime)) THEN 0
            ELSE NULL
        END AS TimeSpanAfter,
		CASE
			WHEN ParameterType = 'Main'
				THEN SUM(CASE WHEN StartTime > DATEADD(HOUR, 12, PrevEndTime) THEN 1 ELSE 0 END) OVER (PARTITION BY PID_PDMS, PID_KISPI, FID_KISPI, ECMO ORDER BY StartTime)  -- i.e. new batch when StartTime > PrevEndTime (+12h), else same batch
		END AS BatchID
    FROM 
        CTE_ECMO_TimeGaps
),
CTE_ECMO_MergedBatches AS ( -- group by patient/ Main Parameter Batch of Main Parameter RangeSignals UNION Side Parameters RangeSignals
    SELECT 
        PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		ECMO,
        Parameter,
        ParameterType,
        MIN(StartTime) AS StartTime,
        MAX(EndTime) AS EndTime,
        SUM(Duration_min) AS Duration_min,
        BatchID
    FROM 
        CTE_ECMO_TimeSpans
	WHERE BatchID IS NOT NULL -- only Main Parameter RangeSignals, which constitute Main Parameter Batches
    GROUP BY 
        PID_PDMS, PID_KISPI, FID_KISPI, ECMO, Parameter, ParameterType, BatchID

	UNION

	SELECT 
		PID_PDMS,
		PID_KISPI,
		FID_KISPI,
		ECMO,
		Parameter,
		ParameterType,
		StartTime,
		EndTime,
		Duration_min,
		BatchID
    FROM 
        CTE_ECMO_TimeSpans
	WHERE BatchID IS NULL -- only Side Parameter RangeSignals, which will be used to correct Main Parameter Batches
),
CTE_ECMO_PotentialCorrections AS ( -- just to check which Side Parameter RangeSignals would be used to correct a Main Parameter Batch (CTE won't be used further)
	SELECT
	*,
	CASE
		WHEN T1.BatchID IS NOT NULL THEN NULL
		WHEN T1.BatchID IS NULL AND EXISTS(
			SELECT 1
			FROM CTE_ECMO_MergedBatches T2
			WHERE T2.PID_PDMS = T1.PID_PDMS
			AND T2.PID_KISPI = T1.PID_KISPI
			AND T2.FID_KISPI = T1.FID_KISPI
			AND T2.ECMO = T1.ECMO
			AND T2.BatchID IS NOT NULL
			AND T1.StartTime < T2.StartTime -- Start of Side Parameter before Start of Main Parameter
			--AND T1.StartTime >= DATEADD(MINUTE, -60, T2.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter // no restriction for StartTime corrections
			AND T1.EndTime > DATEADD(HOUR, -12, T2.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 12h
		) THEN 'StartTime'
		WHEN T1.BatchID IS NULL AND EXISTS(
			SELECT 1
			FROM CTE_ECMO_MergedBatches T2
			WHERE T2.PID_PDMS = T1.PID_PDMS
			AND T2.PID_KISPI = T1.PID_KISPI
			AND T2.FID_KISPI = T1.FID_KISPI
			AND T2.ECMO = T1.ECMO
			AND T2.BatchID IS NOT NULL
			AND T1.EndTime > T2.EndTime -- End of Side Parameter after End of Main Parameter
			AND T1.EndTime <= DATEADD(MINUTE, 60, T2.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
			AND T1.StartTime < DATEADD(HOUR, 12, T2.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 12h
		) THEN 'EndTime'
		WHEN T1.BatchID IS NULL AND EXISTS(
			SELECT 1
			FROM CTE_ECMO_MergedBatches T2
			WHERE T2.PID_PDMS = T1.PID_PDMS
			AND T2.PID_KISPI = T1.PID_KISPI
			AND T2.FID_KISPI = T1.FID_KISPI
			AND T2.ECMO = T1.ECMO
			AND T2.BatchID IS NOT NULL
			AND T1.StartTime < T2.StartTime -- Start of Side Parameter before Start of Main Parameter
			--AND T1.StartTime >= DATEADD(MINUTE, -60, T2.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter // no restriction for StartTime corrections
			AND T1.EndTime > DATEADD(HOUR, -12, T2.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 12h
			AND T1.EndTime > T2.EndTime -- End of Side Parameter after End of Main Parameter
			AND T1.EndTime <= DATEADD(MINUTE, 60, T2.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
			AND T1.StartTime < DATEADD(HOUR, 12, T2.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 12h
		) THEN 'StartTime & EndTime'
		END AS UsedToCorrect
	FROM CTE_ECMO_MergedBatches T1
),
CTE_ECMO_Correction AS ( -- correct Main Parameter Batches with Side Parameter RangeSignals if present
	SELECT
		M.PID_PDMS,
		M.PID_KISPI,
		M.FID_KISPI,
		M.ECMO,
		COALESCE( --if an earlier overlapping Side Parameter RangeSignal present: take this
			(SELECT TOP 1 S.StartTime
			FROM CTE_ECMO_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.ECMO = M.ECMO
			AND S.ParameterType = 'Side'
			AND S.StartTime < M.StartTime -- Start of Side Parameter before Start of Main Parameter
			--AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter // no restriction for StartTime corrections
			AND S.EndTime > DATEADD(HOUR, -12, M.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 12h
			ORDER BY S.StartTime ASC), M.StartTime
		) AS StartTime,
		COALESCE( --if a later overlapping Side Parameter RangeSignal present: take this
			(SELECT TOP 1 S.EndTime
			FROM CTE_ECMO_MergedBatches S
			WHERE S.PID_PDMS = M.PID_PDMS
			AND S.PID_KISPI = M.PID_KISPI
			AND S.FID_KISPI = M.FID_KISPI
			AND S.ECMO = M.ECMO
			AND S.ParameterType = 'Side'
			AND S.EndTime > M.EndTime -- End of Side Parameter after End of Main Parameter
			AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
			AND S.StartTime < DATEADD(HOUR, 12, M.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 12h
			ORDER BY S.EndTime DESC), M.EndTime
		) AS EndTime,
		DATEDIFF(MINUTE,
			COALESCE( -- whatever will be the StartTime of a Batch (corrected or not)
				(SELECT TOP 1 S.StartTime
				FROM CTE_ECMO_MergedBatches S
				WHERE S.PID_PDMS = M.PID_PDMS
				AND S.PID_KISPI = M.PID_KISPI
				AND S.FID_KISPI = M.FID_KISPI
				AND S.ECMO = M.ECMO
				AND S.ParameterType = 'Side'
				AND S.StartTime < M.StartTime -- Start of Side Parameter before Start of Main Parameter
				--AND S.StartTime >= DATEADD(MINUTE, -60, M.StartTime) -- but Start of Side Parameter still in 60min-correction range before Start of Main Parameter // no restriction for StartTime corrections
				AND S.EndTime > DATEADD(MINUTE, -12, M.StartTime) -- End of Side Parameter is optimally after Start of Main Parameter (overlapping), but ok to have a gap up to 12h
				ORDER BY S.StartTime ASC), M.StartTime),
			COALESCE( -- whatever will be the EndTime of a Batch (corrected or not)
				(SELECT TOP 1 S.EndTime
				FROM CTE_ECMO_MergedBatches S
				WHERE S.PID_PDMS = M.PID_PDMS
				AND S.PID_KISPI = M.PID_KISPI
				AND S.FID_KISPI = M.FID_KISPI
				AND S.ECMO = M.ECMO
				AND S.EndTime > M.EndTime -- End of Side Parameter after End of Main Parameter
				AND S.EndTime <= DATEADD(MINUTE, 60, M.EndTime) -- but End of Side Parameter still in 60min-correction range after End of Main Parameter
				AND S.StartTime < DATEADD(HOUR, 12, M.EndTime) -- Start of Side Parameter is optimally before End of Main Parameter (overlapping), but ok to have a gap up to 12h
				ORDER BY S.EndTime DESC), M.EndTime)
		) AS Duration_min -- no better way to calculate duration in the same CTE (alternative: new CTE)
			
	FROM CTE_ECMO_MergedBatches M
	WHERE M.ParameterType = 'Main'
)

SELECT *
FROM CTE_ECMO_Correction
ORDER BY PID_PDMS, StartTime --order by either ParameterID & StartTime, or by Parameter & StartTime


