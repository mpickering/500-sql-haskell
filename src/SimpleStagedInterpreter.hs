{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module SimpleStagedInterpreter where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)
import Control.Monad
import Data.List
import Data.Maybe
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import Prelude hiding (Applicative(..))
import Instances.TH.Lift ()

class Ops r where
  eq :: Eq a => r a -> r a -> r Bool
  neq :: Eq a => r a -> r a -> r Bool
  bc_split :: r Char -> r ByteString -> r [ByteString]

instance Ops Code where
  eq (Code e1) (Code e2) = Code [|| $$e1 == $$e2 ||]
  neq (Code e1) (Code e2) = Code [|| $$e1 /= $$e2 ||]
  bc_split (Code e1) (Code e2) = Code [|| BC.split $$e1 $$e2 ||]

newtype Code a = Code (Q (TExp a))

runCode :: Code a -> QTExp a
runCode (Code a) = a


pure :: Lift a => a -> Code a
pure = Code . unsafeTExpCoerce . lift

infixl 4 <*>
(<*>) :: Code (a -> b) -> Code a -> Code b
(Code f) <*> (Code a) = Code [|| $$f $$a ||]

type Fields = [QTExp ByteString]
type Schema = [ByteString]
type Res = IO ()
type Table = FilePath

data Record = Record { fields :: Fields, schema :: Schema }

getField :: ByteString -> Record -> QTExp ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)

getFields :: [ByteString] -> Record -> [QTExp ByteString]
getFields fs r = map (flip getField r) fs


data Operator = Scan FilePath Schema | Print Operator | Project Schema Schema Operator
              | Filter Predicate Operator | Join Operator Operator deriving Show

query :: Operator
query = Project ["age"] ["age"] (Filter (Eq (Value "john") (Field "name")) (Scan "data/test.csv" ["name", "age"]))

query2 :: Operator
query2 = Project ["name"] ["name"] (Filter (Eq (Value "34") (Field "age")) (Scan "data/test.csv" ["name", "age"]))

queryJoin :: Operator
queryJoin = Join (Scan "data/test.csv" ["name", "age"]) (Scan "data/test1.csv" ["name", "weight"])


data Predicate = Eq Ref Ref | Ne Ref Ref deriving Show

data Ref = Field ByteString | Value ByteString deriving Show

type QTExp a = Q (TExp a)

fix :: (a -> a) -> a
fix f = let x = f x in x

parseRow' :: Schema -> QTExp ByteString -> QTExp ByteString
parseRow' [] b = b
parseRow' [_] b = [|| (BC.tail (BC.dropWhile (/= '\n') $$b)) ||]
parseRow' (_:ss) b = parseRow' ss [||  (BC.tail (BC.dropWhile (/= ',') $$b)) ||]

processCSV :: Schema -> FilePath -> QTExp (IO [Record])
processCSV ss fp =
  [|| do
        bs <- B.readFile fp
        return $ $$(rows ss) bs ||]
  where
    rows :: Schema -> QTExp (ByteString -> [Record])
    rows sch = do
      [||
        fix (\r rs ->
          case rs of
            "" -> []
            _ ->
              $$(let (_, fs) = parseRow sch [||rs||]
                     Code head_rec = pure (Record fs sch)
                 in head_rec) : r $$(parseRow' sch [||rs||]) )||]


    parseRow :: Schema -> QTExp ByteString -> (QTExp ByteString, [QTExp ByteString])
    parseRow [] b = (b, [])
    parseRow [_] b =
      ([|| let res = BC.dropWhile (/= '\n') $$b in BC.tail res ||]
      , [[|| BC.takeWhile (/= '\n') $$b ||]])

    parseRow (_:ss') b =
      let new = [|| let res = BC.dropWhile (/= ',') $$b in BC.tail res ||]
          (final, rs) = parseRow ss' new
      in (final, [|| BC.takeWhile (/= ',') $$b ||] : rs)

evalPred :: Predicate -> Record -> Bool
evalPred predicate rec =
  case predicate of
    Eq a b -> (==) (evalRef a rec) (evalRef b rec)
    Ne a b -> (/=) (evalRef a rec) (evalRef b rec)

evalRef :: Ref -> Record -> ByteString
evalRef (Value a) _ = a
evalRef (Field name) r = getField name r

filterC :: (Record -> Code Bool) -> [Record] -> Code [Record]
filterC f [] = Code [|| [] ||]
filterC f (x:xs) = Code
  [|| if $$(runCode $ f x) then x : $$(runCode $ filterC f xs) else $$(runCode $ filterC f xs) ||]

--filterC2 :: Lift a => (Code a -> Code Bool) -> Code [a] -> Code [a]
--filterC2 f xs = Code [|| filter (\a -> $$(runCode $ f (Code [|| a ||]))) $$(runCode xs) ||]


restrict :: Record -> Schema -> Schema -> Record
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema

execOp :: Operator -> QTExp (IO [Record])
execOp op  =
  case op of
    Scan filename sch -> processCSV sch filename
    --Print p       -> execOp p (printFields . fields)
    Filter predicate parent -> [|| do
      rs <- $$(execOp parent)
      return $ filter (evalPred predicate) rs ||]
    {-
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
                       -}


runQuery :: Operator -> IO ()
runQuery o = execOp o >>= print

main :: IO ()
main = do
--  processCSV "data/test.csv" (print . getField "name")
  execOp query >>= print



