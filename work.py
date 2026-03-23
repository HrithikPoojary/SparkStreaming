%fs ls file:/

dbutils.help()

dbutils.help('fs')

dbutils.fs.help("cp")

dbutils.fs.help("head")

%sql

merge target_table i
using source_table s 
on i.id = s.id 
when matched then
        update
when not matched then
        insert