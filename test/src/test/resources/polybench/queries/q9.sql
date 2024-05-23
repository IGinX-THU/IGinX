CREATE FUNCTION UDTF "extractYear" FROM "UDFExtractYear" IN "test/src/test/resources/polybench/udf/udtf_extract_year.py";

insert into tmpTableC(key, orderkey, year) values (
  select o_orderkey, extractYear(o_orderdate) from mongotpch.orders
);

select * from (
  select
      nation,
      o_year,
      sum(amount) as sum_profit
  from (
           select
               nation.n_name as nation,
               tmpTableC.year as o_year,
               mongotpch.lineitem.l_extendedprice * (1 - mongotpch.lineitem.l_discount) - tpchdata.partsupp.ps_supplycost * mongotpch.lineitem.l_quantity as amount
           from
               tpchdata.part
                   join mongotpch.lineitem on tpchdata.part.p_partkey = mongotpch.lineitem.l_partkey
                   join tpchdata.supplier on tpchdata.supplier.s_suppkey = mongotpch.lineitem.l_suppkey
                   join tpchdata.partsupp on tpchdata.partsupp.ps_suppkey = mongotpch.lineitem.l_suppkey and tpchdata.partsupp.ps_partkey = mongotpch.lineitem.l_partkey
                   join mongotpch.orders on mongotpch.orders.o_orderkey = mongotpch.lineitem.l_orderkey
                   join nation on tpchdata.supplier.s_nationkey = nation.n_nationkey
                   join tmpTableC on mongotpch.orders.o_orderkey = tmpTableC.orderkey
           where
                   tpchdata.part.p_name like '.*green.*'
       )
  group by
      o_year,
      nation
  order by
      o_year desc
)
order by nation;