-- benchling - unsplited json file (parsed_json.csv)
-- copies - cnv with processed nan (cnv_processed_nan_processed.txt)
-- upGenes - info about upgenes from json (up_parsed_json.csv)
-- downGenes - info about downgenes from json (down_parsed_json.csv)


# nan placeholders in copies 

# type_alternation NOSYMBOL
# copy_ratio 0
# log_copy_ratio 0
# copy_number -1
# flank_geneIds 0
# symbol NOSYMBOL

SET SQL_SAFE_UPDATES = 0;

UPDATE copies  
SET type_alternation = NULL  
WHERE type_alternation = 'NOSYMBOL' ;

UPDATE copies  
SET copy_ratio = NULL  
WHERE copy_ratio = 0;

UPDATE copies  
SET log_copy_ratio = NULL  
WHERE log_copy_ratio = 0 ;

UPDATE copies  
SET copy_number = NULL  
WHERE copy_number = -1 ;

UPDATE copies  
SET flank_geneIds = NULL  
WHERE flank_geneIds = 0;

UPDATE copies  
SET symbol = NULL  
WHERE symbol = 'NOSYMBOL' ;



-- a) Number of patients in Benchling with information for genes to up/down regulate

SELECT COUNT(*)
FROM(
	SELECT patID
	FROM upGenes
	GROUP BY patID) AS S;

# OUTPUT : 10

-- b) Number of patients with information for copy number variation 

SELECT COUNT(*)
FROM (
	SELECT COUNT(*)
	FROM copies
	GROUP BY Patient_ID) as S;

# OUTPUT : 2

-- c) Identify which patients have both information

SELECT Patient_ID
FROM (
	SELECT Patient_ID
	FROM copies
	GROUP BY Patient_ID) as S
WHERE Patient_ID IN (
	SELECT patID
 FROM upGenes
);

# OUTPUT : Pat027, Pat007

-- d) For each patient found in “c” list the genes present in Benchling and having a copy number variation found with the tool “pipeline_name: sequenza_vivan”

SELECT wholetable.Patient_ID, wholetable.symbol
FROM(
SELECT Patient_ID, symbol
FROM (
	SELECT Patient_ID,symbol
	FROM copies
	WHERE pipeline_name LIKE '%sequenza_vivan%'
) AS pipeline
WHERE Patient_ID IN (
	# patients that are in benchling&copies
	SELECT Patient_ID
	FROM (
		SELECT Patient_ID
		FROM copies
        WHERE copy_number > 2
		GROUP BY Patient_ID) as groupedCopies
	WHERE Patient_ID IN (
		SELECT patID
		FROM upGenes
	)
) AND symbol IN (
	SELECT upHsgene
    FROM upGenes
)) AS wholetable, upGenes
WHERE wholetable.Patient_ID=upGenes.patID AND wholetable.symbol=upGenes.upHsgene;

# For upGenes
# OUTPUT : Pat027 - GNAS, AURKA, TSHZ2, TSHZ3, CCNE1
#          Pat007 - SMC1A

SELECT wholetable.Patient_ID, wholetable.symbol
FROM(
SELECT Patient_ID, symbol
FROM (
	SELECT Patient_ID,symbol
	FROM copies
	WHERE pipeline_name LIKE '%sequenza_vivan%'
) AS pipeline
WHERE Patient_ID IN (
	# patients that are in benchling&copies
	SELECT Patient_ID
	FROM (
		SELECT Patient_ID
		FROM copies
        WHERE copy_number > 2
		GROUP BY Patient_ID) as groupedCopies
	WHERE Patient_ID IN (
		SELECT patID
		FROM benchling
	)
) AND symbol IN (
	SELECT downHsgene
    FROM downGenes
)) AS wholetable, downGenes
WHERE wholetable.Patient_ID=downGenes.patID AND wholetable.symbol=downGenes.downHsgene;

# For downGenes
# OUTPUT : Pat027 - TP53, KMT2C
#          Pat007 - KDM6A, CEBPA, SMARCA4, MYO1C, KMT2D, PTEN, STAG1