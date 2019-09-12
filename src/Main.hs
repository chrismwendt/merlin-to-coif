module Main where

import           Text.RawString.QQ
import           Control.Applicative
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Lazy          as BSL
import qualified Data.ByteString.Char8         as BSC
import qualified Data.ByteString.Lazy.Char8    as BSLC
import qualified Data.Attoparsec.ByteString.Char8
                                               as A
import Data.ByteString.Builder (toLazyByteString)
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
import qualified Data.Text.Lazy                as TL
import qualified Data.Text.Lazy.IO             as TL
import qualified Data.Text.IO                  as T
import qualified Data.Text.Encoding            as T
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
import           Data.Aeson ((.=), FromJSON, ToJSON, object)
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Text as Aeson
import Control.Concurrent (threadDelay)

main :: IO ()
main = do
  csvFileGlob <- (?: "") <$> lookupEnv "csvFileGlob"
  out         <- (?: "") <$> lookupEnv "out"
  profile     <- (== Just "true") <$> lookupEnv "profile"
  run (Args { csvFileGlob, out, profile })

data Args = Args {csvFileGlob :: String, out :: String, profile :: Bool} deriving (Show)

ghci :: IO ()
ghci = void $ do
  -- run (Args { csvFileGlob = "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/*.csv", out = "out.jsonl", profile = False })
  run (Args { csvFileGlob = "/Users/chrismwendt/github.com/tree-sitter/tree-sitter/output/e982a0fadaffe985944b3968e19f4ee997e89389.dbda9823d00ee7aa42c5cf99252b58b09c3bccfe.csv", out = "out.jsonl", profile = False })
  -- run (Args { csvFileGlob = "/Users/chrismwendt/github.com/sourcegraph/lsif-cpp/examples/cross-app/output/*.csv", out = "out.jsonl", profile = False })

run :: Args -> IO ()
run (Args { csvFileGlob, out, profile }) = do
  when (csvFileGlob == "") $ putStrLn "Must specify --csvFileGlob" >> exitFailure
  when (out == "") $ putStrLn "Must specify --out" >> exitFailure

  let
    maybeTimeIt = if profile then timeIt else (\_ -> id)
    texecute_ conn q = maybeTimeIt (show q) $ execute_ conn q

  removeIfExists "scratch.db"
  withConnection "scratch.db" $ \conn -> do
    execute_ conn "PRAGMA foreign_keys = ON"

    execute_ conn "CREATE TABLE symbols (id INTEGER PRIMARY KEY, name TEXT NOT NULL, deffile TEXT NOT NULL, defrange TEXT NOT NULL, UNIQUE (name, deffile, defrange), CHECK (name != '' OR (deffile != '' AND defrange != '')), CHECK ((deffile = '') = (defrange = '')))"
    execute_ conn "CREATE INDEX symbols_deffile_defrange on symbols(deffile, defrange)"
    execute_ conn "CREATE TABLE refs (name TEXT NOT NULL, deffile TEXT NOT NULL, defrange TEXT NOT NULL, reffile TEXT NOT NULL, refrange TEXT NOT NULL, UNIQUE (reffile, refrange))"
    execute_ conn "CREATE TABLE decldef (reffile TEXT NOT NULL, refrange TEXT NOT NULL, deffile TEXT NOT NULL, defrange TEXT NOT NULL, UNIQUE (reffile, refrange))"

    files <- glob csvFileGlob
    when (files == []) $ putStrLn (csvFileGlob <> " did not match any files!") >> exitFailure
    let
      nFiles = length files
    putStrLn $ "Converting " <> show nFiles <> " CSV file(s) into one " <> out <> " LSIF file..."
    forM
      (zip files [0 .. ])
      (\(file, index) -> do
        putStrLn $ "Loading file " <> show (index + 1) <> "/" <> show nFiles <> " " <> file <> "..."
        input  <- BS.readFile file
        result <- maybeTimeIt ("parse " ++ file) $ return $! A.parseOnly fileP input
        case result of
          Left  err  -> putStrLn err
          Right rows -> do
            maybeTimeIt ("insert " ++ file) $ withTransaction conn $ forM_ rows $ \r@(Row kind kvs) -> do
              let name = if kind == "variable" && "scopequalname" `Map.member` kvs then "" else fromJust $ msum $ map (kvs !?) ["qualname", "name"]
                  def  = execute conn "INSERT INTO symbols (name, deffile, defrange) VALUES (?, ?, ?) ON CONFLICT DO NOTHING" (name, getFile (kvs ! "loc"), getRange (kvs ! "loc", kvs ! "locend"))
              case kind of
                "type"     -> def
                "typedef"  -> def
                "function" -> def
                "macro"    -> def
                "variable" -> def
                "decldef"  -> when ("defloc" `Map.member` kvs) $ execute conn "INSERT INTO decldef (reffile, refrange, deffile, defrange) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING" (getFile (kvs ! "loc"), getRange (kvs ! "loc", kvs ! "locend"), getFile (kvs ! "defloc"), getRange (kvs ! "defloc", kvs ! "deflocend"))
                "ref"      -> execute conn "INSERT INTO refs (name, deffile, defrange, reffile, refrange) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING" (name, (getFile <$> (kvs !? "defloc")) ?: "", (getRange <$> ((,) <$> (kvs !? "defloc") <*> (kvs !? "deflocend"))) ?: "", getFile (kvs ! "loc"), getRange (kvs ! "loc", kvs ! "locend"))
                "include"  -> return ()
                "warning"  -> return ()
                "call"     -> return ()
                _          -> putStrLn $ "UNIMPLEMENTED " ++ show r
      )

    -- apply decldefs to connect defs and refs
    putStrLn "Connecting references..."
    texecute_ conn $ Query $ T.pack $ [r|
      UPDATE refs SET
        deffile  = (SELECT d.deffile  FROM decldef d WHERE d.reffile = refs.deffile AND d.refrange = refs.defrange),
        defrange = (SELECT d.defrange FROM decldef d WHERE d.reffile = refs.deffile AND d.refrange = refs.defrange)
      WHERE (deffile, defrange) IN (SELECT reffile, refrange FROM decldef)
    |]
    texecute_ conn "DROP TABLE decldef"

    -- erase the names of local variables in the refs table
    putStrLn "Computing local symbols..."
    texecute_ conn $ Query $ T.pack $ [r|
      UPDATE refs SET name = (SELECT name FROM symbols WHERE refs.deffile = symbols.deffile AND refs.defrange = symbols.defrange AND refs.deffile != '')
      WHERE EXISTS (SELECT * FROM symbols WHERE refs.deffile = symbols.deffile AND refs.defrange = symbols.defrange AND refs.deffile != '')
    |]

    -- forget the locations of defs in header files
    putStrLn "Computing external symbols..."
    texecute_ conn $ Query $ T.pack $ [r|
      UPDATE refs SET deffile = '', defrange = ''
      WHERE NOT EXISTS (SELECT * FROM symbols WHERE refs.deffile = symbols.deffile AND refs.defrange = symbols.defrange)
    |]

    putStrLn $ "Dumping to " <> out <> "..."
    maybeTimeIt "emit" $ emit conn out

    putStrLn $ "Done!"
    -- TL.putStrLn =<< TL.readFile out

emit :: Connection -> String -> IO ()
emit conn out = void $ do
  removeIfExists out
  writeFile out ""

  fold_
    conn
    "SELECT name, deffile, defrange, reffile, json_group_array(refrange) FROM refs GROUP BY name, deffile, defrange, reffile"
    ("", "", "")
    (\old@(oldname, olddeffile, olddefrange) ((name, deffile, defrange, reffile, refranges) :: (Text, Text, Text, Text, Text)) -> do
      let
        emitJSON = TL.appendFile out . (`TL.snoc` '\n') . Aeson.encodeToLazyText
      when (old /= (name, deffile, defrange)) $ do
        case (name, deffile) of
          ("", "") -> putStrLn "bug in lsif-cpp: either name or deffile are expected to be populated"
          (_, "") -> emitJSON $ object [ "symbol" .= object [ "name" .= name ] ]
          ("", _) -> emitJSON $ object [ "symbol" .= object [ "file" .= deffile, "range" .= defrange ] ]
          (_, _) -> emitJSON $ object [ "symbol" .= object [ "name" .= name, "file" .= deffile, "range" .= defrange ] ]
      emitJSON $ object [ "references" .= object [ reffile .= fromJust (decodeJSON refranges :: Maybe [Text]) ] ]
      return (name, deffile, defrange)
    )

type Parser a = A.Parser a

data Row = Row Text (Map Text Text) deriving (Eq, Ord)

instance Show Row where
  show (Row kind kvs) = "(" ++ T.unpack kind ++ ") " ++ unwords (map (\(k, v) -> T.unpack k ++ ":" ++ T.unpack v) (Map.toList kvs))

fileP :: Parser [Row]
fileP = rowP `M.sepEndBy` "\n"

rowP :: Parser Row
rowP = do
  let keyP :: Parser Text
      keyP = T.decodeLatin1 <$> A.takeWhile (\c -> ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_')
      quotedP :: Parser Text
      quotedP = T.decodeLatin1 . BSC.pack <$> (A.char '"' *> A.many' (('"' <$ "\"\"") <|> (A.char '\\' *> A.anyChar) <|> A.notChar '"') <* A.char '"')
      kvP :: Parser (Text, Text)
      kvP = (,) <$> keyP <* A.char ',' <*> quotedP
  kind <- keyP
  A.char ','
  keyvals <- kvP `M.sepBy` A.char ','
  return $ Row kind (Map.fromList keyvals)

getFile :: Text -> Text
getFile s = case reverse $ T.splitOn ":" s of
  (column : row : file) -> T.intercalate ":" file
  _ -> ""

getRange :: (Text, Text) -> Text
getRange (s1, s2) = case (reverse $ T.splitOn ":" s1, reverse $ T.splitOn ":" s2) of
  (column1 : row1 : _, column2 : row2 : _) -> row1 <> ":" <> column1 <> "-" <> row2 <> ":" <> column2
  _ -> ""

removeIfExists :: FilePath -> IO ()
removeIfExists fileName =  removeFile fileName `Exception.catch` (\(_ :: IOError) -> return ())
 where
  handleExists e | isDoesNotExistError e = return ()
                 | otherwise             = Exception.throwIO e

decodeJSON :: FromJSON a => Text -> Maybe a
decodeJSON = Aeson.decode . toLazyByteString . T.encodeUtf8Builder

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
