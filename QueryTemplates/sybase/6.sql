SELECT
	SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM
	LINEITEM
WHERE
            L_EXTENDEDPRICE :varies