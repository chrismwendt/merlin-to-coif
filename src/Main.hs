module Main where

import Data.List.Extra (stripSuffix)
import qualified Data.List as List
import GHC.Generics
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
  inFileGlob <- (?: "") <$> lookupEnv "inFileGlob"
  root       <- (?: "") <$> lookupEnv "root"
  out        <- (?: "") <$> lookupEnv "out"
  run (Args { inFileGlob, root, out })

data Args = Args {inFileGlob :: String, root :: String, out :: String} deriving (Show)

ghci :: IO ()
ghci = void $ do
  run (Args {
    -- inFileGlob = "/Users/chrismwendt/patdiff-build/duniverse/patdiff/lib/patdiff_format.ml.lsif.in",
    -- inFileGlob = "/Users/chrismwendt/github.com/sourcegraph/merlin-to-lsif-converter/examples/two/comparison_result.ml.lsif.in",
    inFileGlob = "/Users/chrismwendt/patdiff-build/duniverse/**/*.ml.lsif.in",
    -- inFileGlob = "/Users/chrismwendt/patdiff-build/duniverse/a.ml.lsif.in",
    root = "/Users/chrismwendt/patdiff-build/duniverse",
    out = "out.jsonl"
  })

run :: Args -> IO ()
run (Args { inFileGlob, root, out }) = do
  when (inFileGlob == "") $ putStrLn "Must specify environment variable inFileGlob" >> exitFailure
  when (root == "") $ putStrLn "Must specify environment variable root" >> exitFailure
  when (out == "") $ putStrLn "Must specify environment variable out" >> exitFailure

  removeIfExists "scratch.db"
  withConnection "scratch.db" $ \conn -> do
    execute_ conn "PRAGMA foreign_keys = ON"
    execute_ conn "PRAGMA synchronous = OFF"
    withTransaction conn $ do
      execute_ conn [r|
        CREATE TABLE refs (
          file TEXT NOT NULL,
          startline INTEGER NOT NULL,
          startcol INTEGER NOT NULL,
          endcol INTEGER NOT NULL,
          deffile TEXT NOT NULL,
          defline INTEGER NOT NULL,
          defcol INTEGER NOT NULL,
          hover TEXT,
          UNIQUE (file, startline, startcol, endcol),
          CHECK (file != ''),
          CHECK (((deffile = '') AND (defline = -1) AND (defcol = -1)) OR ((deffile != '') AND (defline != -1) AND (defcol != -1)))
        )
      |]
      execute_ conn "CREATE INDEX refs_ix on refs(deffile, defline, defcol)"
      execute_ conn "CREATE INDEX refs_ix2 on refs(deffile, defline)"

      files <- glob inFileGlob
      when (files == []) $ putStrLn (inFileGlob <> " did not match any files!") >> exitFailure
      let
        nFiles = length files
      putStrLn $ "Converting " <> show (length files) <> " *.lsif.in file(s) into one " <> out <> " CoIF file..."
      forM
        (zip files [0 .. ])
        (\(globFile, index) -> do
          let
            file = case (root ++ "/") `List.stripPrefix` globFile of
              Nothing -> error $ "File " ++ globFile ++ " is not under root " ++ root
              Just v -> v
          putStrLn $ "Loading file " <> show (index + 1) <> "/" <> show nFiles <> " " <> file <> "..."
          input <- filter (not . ("{\"class" `BSL.isPrefixOf`)) . BSLC.lines <$> BSLC.readFile globFile
          case zipWithM ((\ix line -> mapLeft (ix, line, ) (Aeson.eitherDecode line :: Either String Line))) [0 .. ] input of
            Left (ix, line, e) -> do
              putStrLn $ "On base-0 line " ++ (show ix) ++ ": " ++ e
              putStrLn $ "| "
              putStrLn $ "| " ++ (BSLC.unpack line)
              putStrLn $ "| "
            Right parsedLines -> do
              let
                sameLine (Line { start = Position { line = startline }, end = Position { line = endline } }) = startline == endline
                toRef (Line { start = Position { line = startline, col = startcol }, end = Position { line = endline, col = endcol }, definition = Definition { dfile = deffile, pos = Position { line = defline, col = defcol } }, lhover = hover }) =
                  case (if deffile == "" then Just "" else T.pack (root ++ "/") `T.stripPrefix` (if deffile == "*buffer*" then T.pack (".lsif.in" `stripSuffix` globFile ?: globFile) else deffile)) of
                    Nothing -> Nothing
                    Just thedeffile -> Just $ Ref { file = T.pack (".lsif.in" `stripSuffix` file ?: file), startline = startline - 1, startcol, endcol, deffile = thedeffile, defline = if defline == -1 then -1 else defline - 1, defcol, hover = T.take 100 <$> hover }
                singleLineRefs = catMaybes $ map toRef $ filter sameLine $ parsedLines
              void $ executeMany
                conn
                [r|

                  INSERT INTO refs (file, startline, startcol, endcol, deffile, defline, defcol, hover)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(file, startline, startcol, endcol) DO UPDATE SET
                          deffile = case when excluded.deffile != '' then excluded.deffile else deffile end,
                          defline = case when excluded.defline != -1 then excluded.defline else defline end,
                          defcol = case when excluded.defcol != -1 then excluded.defcol else defcol end,
                          hover = COALESCE(excluded.hover, hover)
                |]
                singleLineRefs
        )

    putStrLn $ "Dropping rows without definitions..."
    execute_ conn "DELETE FROM refs WHERE deffile = ''"

    -- putStrLn $ "Dropping outer ranges that are overridden by inner ranges..."
    -- execute_ conn [r|
    --   DELETE FROM refs
    --   WHERE EXISTS (
    --     SELECT * FROM refs r2
    --     WHERE
    --       refs.file = r2.file AND
    --       refs.startline = r2.startline AND
    --       (
    --         (refs.startcol < r2.startcol AND r2.endcol <= refs.endcol) OR
    --         (refs.startcol <= r2.startcol AND r2.endcol < refs.endcol)
    --       )
    --   )
    -- |]

    putStrLn $ "Dumping to " <> out <> "..."
    emit conn out

    putStrLn $ "Done!"
    -- TL.putStrLn =<< TL.readFile out

emit :: Connection -> String -> IO ()
emit conn out = void $ do
  removeIfExists out
  writeFile out ""

  fold_
    conn
    "select deffile, defline, defcol, file, hover, json_group_array(startline), json_group_array(startcol), json_group_array(endcol) from refs group by deffile, defline, defcol, file"
    ("", -1, -1)
    (\old@(olddeffile, olddefline, olddefcol) ((deffile, defline, defcol, reffile, hover, refstartlines, refstartcols, refendcols) :: (Text, Int, Int, Text, Maybe Text, Text, Text, Text)) -> do
      let
        emitJSON = TL.appendFile out . (`TL.snoc` '\n') . Aeson.encodeToLazyText
      when (old /= (deffile, defline, defcol)) $ emitJSON $ object [ "symbol" .= object [ "file" .= deffile, "range" .= (T.pack (show defline) <> ":" <> T.pack (show defcol) <> "-" <> T.pack (show (defcol + 1))), "hover" .= hover ] ]
      let
        ints y = case (decodeJSON y) :: Maybe [Int] of
          Nothing -> error $ "could not decode as [Int] " ++ show y
          Just v -> v
      emitJSON $ object [ "references" .= object [ "file" .= reffile, "ranges" .= zipWith3 (\startline startcol endcol -> T.pack (show startline) <> ":" <> T.pack (show startcol) <> "-" <> T.pack (show endcol)) (ints refstartlines) (ints refstartcols) (ints refendcols) ] ]
      return (deffile, defline, defcol)
    )

data Position = Position { line :: Int, col :: Int } deriving (Eq, Ord, Show, Generic)
data Definition = Definition { dfile :: Text, pos :: Position } deriving (Eq, Ord, Show, Generic)
data Line = Line { start :: Position, end :: Position, definition :: Definition, lhover :: Maybe Text } deriving (Eq, Ord, Show, Generic)
data Ref = Ref { file :: Text, startline :: Int, startcol :: Int, endcol :: Int, deffile :: Text, defline :: Int, defcol :: Int, hover :: Maybe Text } deriving (Eq, Ord, Show)

instance ToRow Ref where
  toRow (Ref { file, startline, startcol, endcol, deffile, defline, defcol, hover }) = toRow (file, startline, startcol, endcol, deffile, defline, defcol, hover)

instance FromJSON Position

instance FromJSON Definition where
  parseJSON = Aeson.withObject "Definition" $ \v -> do
    dfile <- v Aeson..: "file"
    pos <- v Aeson..: "pos"
    return $ Definition { dfile, pos }

instance FromJSON Line where
  parseJSON = Aeson.withObject "Line" $ \v -> do
    start <- v Aeson..: "start"
    end <- v Aeson..: "end"
    definition <- v Aeson..:? "definition" Aeson..!= (Definition { dfile = "", pos = Position { line = -1, col = -1 } })
    lhover <- v Aeson..:? "type"
    return $ Line { start, end, definition, lhover }

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

mapLeft :: (a -> c) -> Either a b -> Either c b
mapLeft f (Left v) = Left (f v)
mapLeft _ (Right v) = Right v
