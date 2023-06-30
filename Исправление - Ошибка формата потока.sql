use Akusherstvo

delete from Config where FileName = 'commit'
delete from Config where FileName = 'dbStruFinal'
delete from Config where FileName = 'DynamicallyUpdated' --(для версии 8.3)
delete from Config where FileName = 'dynamicCommit' --(для версии 8.3)
delete from ConfigSave