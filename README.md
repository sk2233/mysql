## 支持的 sql
```sql
select id,height from users
select * from users where id > 20 AND id < 30
select id,name from users where id > 20 order by id desc
select distinct id,name from users
select id,name from users limit 10 offset 8
select name,count(id) from users where id > 30 group by name  -- 这里 count 不支持 * 必须使用字段
select users.id,users.name,stud.uid,stud.height from users join stud on users.id = stud.uid where stud.uid < 100  -- JOIN 使用字段必须指定表名

update stud set name = 'mysql',extra = 'a db' where uid > 100
insert into stud values(1,22,'hello','world'),(2,33,'my','sql')  -- 必须填写全字段，不支持默认值
delete from stud where id = 1

CREATE TABLE stud(uid int,height float,name varchar(32),extra text)
CREATE INDEX stud_idx ON stud(height,name)
```
## 支持的指令
```shell
begin 
commit 
rollback 
exit
```
## 参考教程
https://coding.imooc.com/class/711.html?mc_marking=de92f3f7813cfffa89e2016a2c4d89df&mc_channel=banner