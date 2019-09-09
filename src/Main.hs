module Main where

import           Text.RawString.QQ
import           Control.Applicative
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Lazy          as BSL
import qualified Data.ByteString.Char8         as BSC
import qualified Data.ByteString.Lazy.Char8    as BSLC
import qualified Data.Attoparsec.ByteString.Char8
                                               as A
import qualified Text.Megaparsec               as M
import qualified Text.Megaparsec.Char          as M
import           Data.Void
import qualified Data.Set                      as Set
import           Data.Set                       ( Set )
import qualified Data.Map.Lazy                 as Map
import           Data.Map.Lazy                  ( Map
                                                , (!)
                                                , (!?)
                                                )
import           Database.SQLite.Simple
import           System.Directory
import qualified Control.Exception             as Exception
import           System.IO.Error
import           Control.Monad
import           Data.Maybe
import           System.FilePath.Glob
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Data.IORef
import           System.Clock                   ( getTime
                                                , Clock(..)
                                                )
import           Formatting                     ( fprint
                                                , (%)
                                                , string
                                                )
import           Formatting.Clock               ( timeSpecs )
import           Data.Typeable
import           Data.Data
import           System.Exit
import           System.Environment
import           Control.Error.Util             ( (?:) )

main :: IO ()
main = do
  csvFileGlob <- getEnv "csvFileGlob"
  out         <- getEnv "out"
  run (Args { csvFileGlob, out })

data Args = Args {csvFileGlob :: String, out :: String} deriving (Show)

ghci :: IO ()
ghci = void $ do
  run (Args { csvFileGlob = "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/e982a0fadaffe985944b3968e19f4ee997e89389.62ef97ce5fa67ae873e77b9f161c699c392fd296.csv", out = "out.jsonl" })

run :: Args -> IO ()
run (Args { csvFileGlob, out }) = do
  when (csvFileGlob == "") $ putStrLn "Must specify --csvFileGlob" >> exitFailure
  when (out == "") $ putStrLn "Must specify --out" >> exitFailure

  removeIfExists "scratch.db"
  withConnection "scratch.db" $ \conn -> do
    execute_ conn "PRAGMA foreign_keys = ON"

    execute_ conn "CREATE TABLE symbols (id INTEGER PRIMARY KEY, name TEXT NOT NULL, defloc TEXT NOT NULL, deflocend TEXT NOT NULL, UNIQUE (name, defloc), CHECK (name != '' OR defloc != ''), CHECK ((defloc = '') = (deflocend = '')))"
    execute_ conn "CREATE INDEX symbols_defloc on symbols(defloc)"
    execute_ conn "CREATE TABLE refs (name TEXT NOT NULL, defloc TEXT NOT NULL, refloc TEXT NOT NULL PRIMARY KEY, reflocend TEXT NOT NULL, CHECK (name != '' OR defloc != ''))"

    -- {"symbol":{"name":"printf"}}
    -- {"ref":"main.cpp:3:4-3:8"}
    -- {"ref":"main.cpp:4:4-4:8"}
    -- {"symbol":{"def":"lib.cpp:2:3"}}
    -- {"ref":"main.cpp:1:1-1:2"}
    -- {"symbol":{"name":"uppercase","def":"lib.cpp:2:3"}}
    -- {"ref":"main.cpp:5:1-5:8"}

    -- TODO try eliminating this by modifying dxr-indexer.cpp
    execute_ conn "CREATE TABLE decldef (refloc TEXT NOT NULL PRIMARY KEY, defloc TEXT NOT NULL)"

    -- files <- glob "/Users/chrismwendt/github.com/sourcegraph/lsif-cpp/examples/five/output/*.csv"
    -- files <- glob "/Users/chrismwendt/github.com/sourcegraph/lsif-cpp/examples/cross-lib/output/*.csv"
    -- files <- glob "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/*.csv"
    files <- glob csvFileGlob
    forM
      files
      (\file -> do
        -- input  <- readFile file
        -- result <- timeIt ("parse " ++ file) $ case M.runParser filePM "blah" input of
        --   Left  err  -> return (Left (M.errorBundlePretty err))
        --   Right rows -> return (Right rows)
        input  <- BS.readFile file
        result <- timeIt ("parse " ++ file) $ case A.parseOnly filePA input of
          Left  err  -> return (Left err)
          Right rows -> return (Right rows)
        case result of
          Left  err  -> putStrLn err
          Right rows -> do
            timeIt ("insert " ++ file) $ withTransaction conn $ forM_ rows $ \r@(Row kind kvs) -> do
              let name = if kind == "variable" && "scopequalname" `Map.member` kvs then "" else fromJust $ msum $ map (kvs !?) ["qualname", "name"]
                  def  = execute conn "INSERT INTO symbols (name, defloc, deflocend) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (name, kvs ! "loc", kvs ! "locend")
              case kind of
                "type"     -> def
                "typedef"  -> def
                "function" -> def
                "macro"    -> def
                "variable" -> def
                "decldef"  -> when ("defloc" `Map.member` kvs) $ execute conn "INSERT INTO decldef (refloc, defloc) VALUES (?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "defloc")
                "ref"      -> execute conn "INSERT INTO refs (name, defloc, refloc, reflocend) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING" (name, (kvs !? "defloc") ?: "", kvs ! "loc", kvs ! "locend")
                "include"  -> return ()
                "warning"  -> return ()
                "call"     -> return ()
                _          -> putStrLn $ "UNIMPLEMENTED " ++ show r
      )

    texecute_ conn $ Query $ T.pack $ unwords ["UPDATE refs SET defloc = (SELECT d.defloc FROM decldef d WHERE d.refloc = refs.defloc)", "         WHERE defloc IN (SELECT refloc FROM decldef)"]
    texecute_ conn "DROP TABLE decldef"

    texecute_ conn "UPDATE refs SET name = (SELECT name FROM symbols WHERE refs.defloc = symbols.defloc AND refs.defloc != '') WHERE EXISTS (SELECT * FROM symbols WHERE refs.defloc = symbols.defloc AND refs.defloc != '')"

    texecute_ conn "UPDATE refs SET defloc = '' WHERE NOT EXISTS (SELECT * FROM symbols WHERE refs.defloc = symbols.defloc)"

    texecute_ conn $ Query $ T.pack $ [r|
      INSERT INTO symbols (name, defloc, deflocend)
                    SELECT refs.name, '', '' FROM refs
                    WHERE refs.defloc = ''
                    ON CONFLICT DO NOTHING
    |]

    texecute_ conn "CREATE TABLE refs_tmp AS SELECT * FROM refs"
    texecute_ conn "DROP TABLE refs"
    texecute_ conn "CREATE TABLE refs (refloc TEXT NOT NULL PRIMARY KEY, reflocend TEXT NOT NULL, sid INTEGER NOT NULL, FOREIGN KEY (sid) REFERENCES symbols(id))"
    texecute_ conn $ Query $ T.pack $ [r|
      INSERT INTO refs (refloc, reflocend, sid)
      SELECT refloc, reflocend, id FROM refs_tmp
      JOIN symbols on
      refs_tmp.name = symbols.name AND
      refs_tmp.defloc = symbols.defloc
    |]
    texecute_ conn "DROP TABLE refs_tmp"

texecute_ :: Connection -> Query -> IO ()
texecute_ conn q = timeIt (show q) $ execute_ conn q

timeIt :: String -> IO a -> IO a
timeIt label a = do
  start <- getTime Monotonic
  r     <- a
  end   <- getTime Monotonic
  fprint (timeSpecs % ": " % Formatting.string % "\n") start end label
  return r

showTable :: Connection -> String -> IO ()
showTable conn table = do
  putStrLn $ "--- " ++ table
  results <- query_ conn (Query $ T.pack $ "SELECT * FROM " ++ table) :: IO [[SQLData]]
  mapM_ print results

sqliteBool :: Bool -> Int
sqliteBool False = 0
sqliteBool True  = 1

type MParser a = M.Parsec Void String a
type AParser a = A.Parser a

data Row = Row BS.ByteString (Map BS.ByteString BS.ByteString) deriving (Eq, Ord)

instance Show Row where
  show (Row kind kvs) = "(" ++ BSC.unpack kind ++ ") " ++ unwords (map (\(k, v) -> BSC.unpack k ++ ":" ++ BSC.unpack v) (Map.toList kvs))

filePM :: MParser [Row]
filePM = rowPM `M.sepEndBy` "\n"

rowPM :: MParser Row
rowPM = do
  let keyP :: MParser String
      keyP = M.some (M.satisfy (\c -> ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_'))
      quotedP :: MParser String
      quotedP = M.char '"' *> M.many (M.try ('"' <$ "\"\"") <|> M.try (M.char '\\' *> M.anySingle) <|> M.anySingleBut '"') <* M.char '"'
      kvP :: MParser (String, String)
      kvP = (,) <$> keyP <* M.char ',' <*> quotedP
  kind <- keyP
  M.char ','
  keyvals <- kvP `M.sepBy` M.char ','
  return $ Row (BSC.pack kind) (Map.fromList $ map (\(k, v) -> (BSC.pack k, BSC.pack v)) keyvals)

filePA :: AParser [Row]
filePA = rowPA `M.sepEndBy` "\n"

rowPA :: AParser Row
rowPA = do
  let keyP :: AParser BS.ByteString
      keyP = A.takeWhile (\c -> ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_')
      quotedP :: AParser BS.ByteString
      quotedP = BSC.pack <$> (A.char '"' *> A.many' (('"' <$ "\"\"") <|> (A.char '\\' *> A.anyChar) <|> A.notChar '"') <* A.char '"')
      kvP :: AParser (BS.ByteString, BS.ByteString)
      kvP = (,) <$> keyP <* A.char ',' <*> quotedP
  kind <- keyP
  A.char ','
  keyvals <- kvP `M.sepBy` A.char ','
  return $ Row kind (Map.fromList keyvals)

removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `Exception.catch` handleExists
 where
  handleExists e | isDoesNotExistError e = return ()
                 | otherwise             = Exception.throwIO e
