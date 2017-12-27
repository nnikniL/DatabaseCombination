# DatabaseCombination
Create a solution for combinate data in different DB files from markets.To combinate type-same DBs, implmenting SQLite first

由于近期爬了很多交易所的数据，故创建该工程。实现同类数据库文件的合并功能，首先实现了SQLite数据库的合并，抄了很多DbCommand内部的代码，实现不是很优雅，可以感受下。


注意：
SQLiteCommand和它的基类，封装得太密了，修改Parameters无法直接达到更新SQL语句的目的，所以就无法方便地通过DataTable批量写入了。考虑到纯insert工作量不大，也用不到DataTable的update等强大功能，采用拼接insert子SQL的方式写数据库了（反正内部估计也是这么干的）。
