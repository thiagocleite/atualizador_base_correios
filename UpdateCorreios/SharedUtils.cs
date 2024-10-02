using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateCorreios
{
    public static class SharedUtils
    {
        public static string mainQuery = $@"SELECT
									     cep,
									     	pais,
									     	uf,
									     	cod_cidade,
									     	cidade,
									     	bairro,
									     	REPLACE(rua,';',',') as rua
									     FROM (
									     	SELECT DISTINCT
									     		L.CEP AS cep,
									     		'BRA' AS Pais,
									     		L.UFE_SG AS UF,
									     		loc.LOC_NU as cod_cidade,
									     		loc.LOC_NO AS Cidade,
									     		B.BAI_NO AS Bairro,
									     		CONCAT(TLO_TX, ' ', LOG_NO) AS rua
									     		,RANK() OVER (PARTITION BY L.CEP,L.UFE_SG,loc.LOC_NU,B.BAI_NO,CONCAT(TLO_TX, ' ', LOG_NO)   ORDER BY B.BAI_NO ASC) AS [rank]  
									     	FROM log_logradouro L
									     	LEFT JOIN log_bairro B 
									     		on --L.LOC_NU = b.LOC_NU
									     		L.BAI_NU_INI = b.BAI_NU
									     		AND L.UFE_SG = B.UFE_SG
									     	LEFT JOIN LOG_localidade Loc
									     		ON L.LOC_NU = Loc.LOC_NU
									     		AND L.UFE_SG = loc.UFE_SG
									     		--AND L.CEP = loc.cep
									     	UNION
									     	SELECT DISTINCT
									     		cid.cep,
									     		'BRA' AS pais,
									     		cid.UFE_SG,
									     		cid.LOC_NU,
									     		cid.LOC_NO cidade,
									     		null bairro,
									     		null as rua,
									     		1 as [rank]
									     	FROM LOG_localidade cid
									     	LEFT JOIN log_logradouro c ON cid.cep = c.cep
									     	WHERE 1=1
									     	AND c.cep IS NULL
									     	AND cid.CEP is not null
									     	
									     	
									     	UNION
									     	SELECT DISTINCT
									     		cpc.cep,
									     		'BRA' AS pais,
									     		cpc.UFE_SG,
									     		cpc.LOC_NU,
									     		L.LOC_NO cidade,
									     		null bairro,
									     		cpc.CPC_ENDERECO as rua,
									     		1 as [rank]
									     	FROM LOG_CPC cpc
									     	LEFT JOIN LOG_LOCALIDADE L
									     		ON L.LOC_NU = cpc.LOC_NU
									     	LEFT JOIN log_logradouro c ON cpc.cep = c.cep
									     	WHERE 1=1
									     	AND c.cep IS NULL
									     	AND cpc.CEP is not null
									     	
									     	UNION
									     	SELECT DISTINCT
									     		GU.cep,
									     		'BRA' AS pais,
									     		GU.UFE_SG,
									     		GU.LOC_NU,
									     		L.LOC_NO cidade,
									     		Ba.BAI_NO bairro,
									     		GU.GRU_ENDERECO as rua,
									     		1 as [rank]
									     	FROM LOG_GRANDE_USUARIO GU
									     	LEFT JOIN LOG_LOCALIDADE L
									     		ON L.LOC_NU = GU.LOC_NU
									     	LEFT JOIN LOG_BAIRRO Ba
									     		ON Ba.BAI_NU = GU.BAI_NU
									     		LEFT JOIN log_logradouro c ON GU.cep = c.cep
									     	WHERE 1=1
									     	AND c.cep IS NULL
									     	AND GU.CEP is not null
									     
									     	UNION
									     	SELECT DISTINCT
									     		UO.cep,
									     		'BRA' AS pais,
									     		UO.UFE_SG,
									     		UO.LOC_NU,
									     		L.LOC_NO cidade,
									     		Ba.BAI_NO bairro,
									     		UO.UOP_ENDERECO as rua,
									     		1 as [rank]
									     	FROM LOG_UNID_OPER UO
									     	LEFT JOIN LOG_LOCALIDADE L
									     		ON L.LOC_NU = UO.LOC_NU
									     	LEFT JOIN LOG_BAIRRO Ba
									     		ON Ba.BAI_NU = UO.BAI_NU
									     		LEFT JOIN log_logradouro c ON UO.cep = c.cep
									     	WHERE 1=1
									     	AND c.cep IS NULL
									     	AND UO.CEP is not null
									     )a
									     WHERE [rank] = 1";
    }
}
