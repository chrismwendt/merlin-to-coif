module Main where

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
import           Text.Megaparsec

main :: IO ()
main = putStr "hello, world\n"

ghci :: IO ()
ghci = void $ do
  removeIfExists "scratch.db"
  withConnection "scratch.db" $ \conn -> do
    execute_ conn "PRAGMA foreign_keys = ON"

    execute_ conn "CREATE TABLE symbols (qualname TEXT NOT NULL, defloc TEXT NOT NULL, PRIMARY KEY (qualname, defloc), CHECK (qualname != '' OR defloc != ''))"
    execute_ conn "CREATE TABLE symrefs (qualname TEXT NOT NULL, defloc TEXT NOT NULL, loc TEXT NOT NULL PRIMARY KEY)"

    execute_ conn "CREATE TABLE defs (loc TEXT NOT NULL PRIMARY KEY, qualname TEXT NOT NULL, toplevel INTEGER NOT NULL CHECK (toplevel IN (0, 1)))"
    execute_ conn "CREATE TABLE ref  (loc TEXT NOT NULL PRIMARY KEY, qualname TEXT NOT NULL, defloc TEXT NOT NULL)"
    execute_ conn "CREATE TABLE decldef (loc TEXT NOT NULL PRIMARY KEY, defloc TEXT NOT NULL)"

    -- files <- glob "/Users/chrismwendt/github.com/sourcegraph/lsif-cpp/examples/cross-app/output/*.csv"
    -- files <- glob "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/*.csv"
    files <- glob "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/8e953de7fce99ed9cd0fe37c8ecd2735133e295b.81fea659b0e5b85f76560c945532bc802e10e90c.csv"
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
              case kind of
                "type"     -> execute conn "INSERT INTO defs (loc, qualname, toplevel) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "qualname", 1 :: Int)
                "typedef"  -> execute conn "INSERT INTO defs (loc, qualname, toplevel) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "qualname", 1 :: Int)
                "decldef"  -> when ("defloc" `Map.member` kvs) $ execute conn "INSERT INTO decldef (loc, defloc) VALUES (?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "defloc")
                "function" -> execute conn "INSERT INTO defs (loc, qualname, toplevel) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "qualname", 1 :: Int)
                "macro"    -> execute conn "INSERT INTO defs (loc, qualname, toplevel) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "name", 1 :: Int)
                "variable" -> execute conn "INSERT INTO defs (loc, qualname, toplevel) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", kvs ! "qualname", sqliteBool (not $ "scopequalname" `Map.member` kvs))
                "ref" ->
                  let qualname = fromJust $ msum $ map (kvs !?) ["qualname", "name"]
                  in  case kvs !? "defloc" of
                        Nothing -> do
                          execute conn "INSERT INTO symbols (qualname, defloc) VALUES (?, '') ON CONFLICT DO NOTHING"         (Only qualname)
                          execute conn "INSERT INTO symrefs (qualname, defloc, loc) VALUES (?, '', ?) ON CONFLICT DO NOTHING" (Just qualname, kvs ! "loc")
                        Just defloc -> execute conn "INSERT INTO ref (loc, qualname, defloc) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (kvs ! "loc", qualname, defloc)
                "include" -> return ()
                "warning" -> return ()
                "call"    -> return ()
                _         -> putStrLn $ "UNIMPLEMENTED " ++ show r
            end <- getTime Monotonic
            fprint ("Inserting " % timeSpecs % " " % Formatting.string % "\n") start end file
      )

    start <- getTime Monotonic
    execute_ conn "UPDATE ref SET defloc = (SELECT d.defloc FROM decldef d WHERE d.loc = ref.defloc) WHERE defloc IN (SELECT d.loc FROM decldef d WHERE d.loc = ref.defloc)"
    execute_ conn "DROP TABLE decldef"

    execute_ conn "INSERT INTO symbols (qualname, defloc) SELECT CASE WHEN defs.toplevel = 1 THEN defs.qualname ELSE '' END, defs.loc FROM defs JOIN ref ON defs.loc = ref.defloc ON conflict DO NOTHING"
    execute_ conn "INSERT INTO symrefs (qualname, defloc, loc) SELECT CASE WHEN defs.toplevel = 1 THEN defs.qualname ELSE '' END, defs.loc, ref.loc FROM defs JOIN ref ON defs.loc = ref.defloc ON conflict DO NOTHING"
    execute_ conn "INSERT INTO symbols (qualname, defloc) SELECT ref.qualname, '' FROM ref LEFT JOIN defs ON defs.loc = ref.defloc WHERE defs.loc IS NULL ON conflict DO NOTHING"
    execute_ conn "INSERT INTO symrefs (qualname, defloc, loc) SELECT ref.qualname, '', ref.loc FROM ref LEFT JOIN defs ON defs.loc = ref.defloc WHERE defs.loc IS NULL ON conflict DO NOTHING"

    execute_ conn "DROP TABLE defs"
    execute_ conn "DROP TABLE ref"
    end <- getTime Monotonic
    fprint ("Finalize " % timeSpecs % "\n") start end

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
