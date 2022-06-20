# Topicos Especiales 2
## Tarea #2
### Problema # 2

1. La	agencia	de	Seguro	Social	(SSA)	del	gobierno	de	los	EEUU	publica	cada	año	una	lista	de	los	nombres	masculinos	y	femeninos	más	
comunes	 de	 recién	 nacidos (<a href="https://www.ssa.gov/cgi-bin/popularnames.cgi" target="_blank"> https://www.ssa.gov/cgi-bin/popularnames.cgi </a> ).
Adjunto	 a	 este	 documento	 encontrará	 el	 archivo	babynames_births2021.csv,	que	contiene	la	lista	para	de	nombres	mas	populares	año	2021 y	
el	numero	total	de	nacimientos	para	niños	y	niñas	con	cada	uno	de	estos	nombres.


2. Escriba	un	script	llamado	*procesaNombres.py* que	lea	el	archivo	y	lo	guarde	en	la	base	de	datos	babynames.sql	de	PostgreSQL.	
Esta base	de	datos	tendrá	una	tabla	con	tres	columnas:	Id,	rango,	nombre,	sexo	(M	o	F)	y	numero	de	nacimientos	para	cada	nombre.
Su script,	mediante	el	uso	de	consultas	SQL,	debe	imprimir	la	siguiente	información:
  + a. Los	45 primeros	nombres	mas	populares	(para	ambos	géneros).
  + b. Todos	los	nombres	que	tienen	4	letras	o	menos	y	sus	posiciones	en	la	lista (para	ambos	géneros).
  + c. Imprima	todos	los	nombres	femeninos	que	tengan	las	letras	‘w’	o	‘x’	o	‘y’	o	‘z’	en	ellas.	

***Sugerencia:	estudie	el	uso	de	la	cláusula	LIKE	(ILIKE)	y	los	operadores	‘%’	y	‘_’.	Donde	El	signo	de	porcentaje	‘%'	representa	
cero,	uno	o	varios	números	o	caracteres.	El	signo	subrayado	'	_	'	representa	un	solo	número	o	carácter.***

  + d. Todos	los	nombres	que	tienen	dos	letras	repetidas	(de	3	casos	distintos),	por	ejemplo:	‘aa’,	‘tt,	‘pp’	(la	letra	p	seguido	de	p).


***Sugerencia:	Aquí	también	puede	usar	una	expresión	regular	para	hacer	el	match***
