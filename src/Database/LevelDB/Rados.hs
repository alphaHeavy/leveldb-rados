module Database.LevelDB.Rados
  ( createRadosEnv
  ) where

import Database.LevelDB (Env(..))
import Database.LevelDB.C (EnvPtr)
import Foreign.C.String (CString, withCString)

type PoolName = String

foreign import ccall safe leveldb_create_rados_env :: CString -> CString -> IO EnvPtr

createRadosEnv :: FilePath -> PoolName -> IO Env
createRadosEnv filePath poolName =
  withCString filePath $ \ filePathStr ->
  withCString poolName $ \ poolNameStr ->
    fmap Env $ leveldb_create_rados_env filePathStr poolNameStr
