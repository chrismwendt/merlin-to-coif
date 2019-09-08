module Main where

import           Text.RawString.QQ
import           Text.Megaparsec.Char
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
import           Control.Error.Util             ( (?:) )
import           Text.Megaparsec

main :: IO ()
main = putStr "hello, world\n"

ghci :: IO ()
ghci = void $ do
  removeIfExists "scratch.db"
  withConnection "scratch.db" $ \conn -> do
    execute_ conn "PRAGMA foreign_keys = ON"

    execute_ conn "CREATE TABLE symbols (id INTEGER PRIMARY KEY, name TEXT NOT NULL, defloc TEXT NOT NULL, UNIQUE (name, defloc), CHECK (name != '' OR defloc != ''))"
    execute_ conn "CREATE INDEX symbols_defloc on symbols(defloc)"
    execute_ conn "CREATE TABLE refs (name TEXT NOT NULL, defloc TEXT NOT NULL, refloc TEXT NOT NULL PRIMARY KEY, CHECK (name != '' OR defloc != ''))"

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
    files <- glob "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/*.csv"
    -- files <- glob "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/e982a0fadaffe985944b3968e19f4ee997e89389.62ef97ce5fa67ae873e77b9f161c699c392fd296.csv"
    forM
      files
      (\file -> do
        input <- readFile file
        start <- getTime Monotonic
        case runParser fileP "blah" input of
          Left  err  -> putStrLn $ errorBundlePretty err
          Right rows -> do
            end <- getTime Monotonic
            fprint ("Parsing " % timeSpecs % " " % Formatting.string % "\n") start end file
            start <- getTime Monotonic
            withTransaction conn $ forM_ rows $ \r@(Row kind kvs) -> do
              let name = if kind == "variable" && "scopequalname" `Map.member` kvs then "" else fromJust $ msum $ map (kvs !?) ["qualname", "name"]
                  def  = execute conn "INSERT INTO symbols (name, defloc) VALUES (?, ?) ON CONFLICT DO NOTHING" (name, kvs ! "loc")
              case kind of
                "type"     -> def
                "typedef"  -> def
                "function" -> def
                "macro"    -> def
                "variable" -> def
                "decldef"  -> when ("defloc" `Map.member` kvs) $ execute conn "INSERT INTO decldef (refloc, defloc) VALUES (?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "defloc")
                "ref"      -> execute conn "INSERT INTO refs (name, defloc, refloc) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (name, (kvs !? "defloc") ?: "", kvs ! "loc")
                "include"  -> return ()
                "warning"  -> return ()
                "call"     -> return ()
                _          -> putStrLn $ "UNIMPLEMENTED " ++ show r
            end <- getTime Monotonic
            fprint ("Inserting " % timeSpecs % " " % Formatting.string % "\n") start end file
      )

    texecute_ conn $ Query $ T.pack $ unwords ["UPDATE refs SET defloc = (SELECT d.defloc FROM decldef d WHERE d.refloc = refs.defloc)", "         WHERE defloc IN (SELECT refloc FROM decldef)"]
    texecute_ conn "DROP TABLE decldef"

    texecute_ conn "UPDATE refs SET name = (SELECT name FROM symbols WHERE refs.defloc = symbols.defloc AND refs.defloc != '') WHERE EXISTS (SELECT * FROM symbols WHERE refs.defloc = symbols.defloc AND refs.defloc != '')"

    texecute_ conn "UPDATE refs SET defloc = '' WHERE NOT EXISTS (SELECT * FROM symbols WHERE refs.defloc = symbols.defloc)"

    texecute_ conn $ Query $ T.pack $ [r|
      INSERT INTO symbols (name, defloc)
                    SELECT refs.name, '' FROM refs
                    WHERE refs.defloc = ''
                    ON CONFLICT DO NOTHING
    |]

    texecute_ conn "CREATE TABLE refs_tmp AS SELECT * FROM refs"
    texecute_ conn "DROP TABLE refs"
    texecute_ conn "CREATE TABLE refs (refloc TEXT NOT NULL PRIMARY KEY, sid INTEGER NOT NULL, FOREIGN KEY (sid) REFERENCES symbols(id))"
    texecute_ conn $ Query $ T.pack $ [r|
      INSERT INTO refs (refloc, sid)
      SELECT refloc, id FROM refs_tmp
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

type Parser a = Parsec Void String a

data Row = Row String (Map String String) deriving (Eq, Ord)

instance Show Row where
  show (Row kind kvs) = "(" ++ kind ++ ") " ++ unwords (map (\(k, v) -> k ++ ":" ++ v) (Map.toList kvs))

fileP :: Parser [Row]
fileP = rowP `sepEndBy` "\n"

rowP :: Parser Row
rowP = do
  let keyP :: Parser String
      keyP = some (satisfy (\c -> ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_'))
      quotedP :: Parser String
      quotedP = char '"' *> many (try ('"' <$ "\"\"") <|> try (char '\\' *> anySingle) <|> anySingleBut '"') <* char '"'
      kvP :: Parser (String, String)
      kvP = (,) <$> keyP <* char ',' <*> quotedP
  kind <- keyP
  char ','
  keyvals <- kvP `sepBy` char ','
  return $ Row kind (Map.fromList keyvals)

removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `Exception.catch` handleExists
 where
  handleExists e | isDoesNotExistError e = return ()
                 | otherwise             = Exception.throwIO e
