{-# LANGUAGE OverloadedStrings #-}
module SimpleInterpreter where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Control.Monad
import Data.List
import Data.Maybe

type Fields = [ByteString]
type Schema = [ByteString]
type Table = FilePath

data Record = Record { fields :: Fields, schema :: Schema } deriving Show



data Operator = Scan Table | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator

query :: Operator
query = Project ["name"] ["name"] (Scan "data/test.csv")

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv") (Scan "data/test1.csv")


data Predicate = Eq Ref Ref | Ne Ref Ref

data Ref = Field ByteString | Value ByteString

processCSV :: FilePath -> IO [Record]
processCSV fp = do
  s <- B.readFile fp
  scanner s

  where
    scanner s = do
      let (fs, rs) = BC.span (/= '\n') s
      return $ rows (BC.split ',' fs) (BC.tail rs)

    rows :: Schema -> ByteString -> [Record]
    rows fs rs =
      case rs of
        "" -> []
        _ ->
          let (r, rest) = BC.span (/= '\n') rs
              r' = (Record (BC.split ',' r) fs)
          in (r' : rows fs (BC.tail rest))


printFields :: Fields -> IO ()
printFields [] = putStr "\n"
printFields [b] = BC.putStr b >> putStr "\n"
printFields (b:bs) = BC.putStr b >> BC.putStr "," >> printFields bs

evalPred :: Predicate -> Record -> Bool
evalPred  predicate rec =
  case predicate of
    Eq a b -> evalRef a rec == evalRef b rec
    Ne a b -> evalRef a rec /= evalRef b rec

evalRef :: Ref -> Record -> ByteString
evalRef (Value a) _ = a
evalRef (Field name) r = getField name r

getField :: ByteString -> Record -> ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in fs !! i

getFields :: [ByteString] -> Record -> [ByteString]
getFields fs r = map (flip getField r) fs

restrict :: [ByteString] -> [ByteString] -> Record -> Record
restrict newSchema parentSchema r =
  Record (getFields parentSchema r) newSchema

execOp :: Operator -> IO [Record]
execOp op  =
  case op of
    Scan filename -> processCSV filename
    --Print p       -> execOp p (printFields . fields)
    Filter predicate parent -> do
      rs <- execOp parent
      return (filter (evalPred predicate) rs)
    Project newSchema parentSchema parent -> do
      rs <- execOp parent
      return $ map (restrict newSchema parentSchema) rs
    Join left right -> do
      ls <- execOp left
      rs <- execOp right
      return $ do
        rec <- ls
        rec' <- rs
        let keys = schema rec `intersect` schema rec'
        guard (getFields keys rec == getFields keys rec')
        (return (Record (fields rec ++ fields rec')
                       (schema rec ++ schema rec')))


runQuery :: Operator -> IO ()
runQuery o = execOp o >>= print

main :: IO ()
main = do
--  processCSV "data/test.csv" (print . getField "name")
  execOp query >>= print



