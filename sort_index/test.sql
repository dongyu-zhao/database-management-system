SELECT SUM(A.c8), SUM(E.c1), SUM(B.c1)
FROM A, B, C, E
WHERE C.c1 = E.c0 AND A.c2 = C.c0 AND A.c1 = B.c0
AND C.c2 > 3797;
