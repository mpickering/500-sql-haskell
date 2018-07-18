{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Compiler where

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


processCSV :: Schema -> Code ByteString -> (Record -> QTExp Res) -> QTExp Res
processCSV ss (Code bs) yld =
  [|| $$(rows ss) $$bs ||]
  where
    rows :: Schema -> QTExp (ByteString -> Res)
    rows sch = do
      [||
        fix (\r rs ->
          case rs of
            "" -> return ()
            _ ->
              $$(let (_, fs) = parseRow sch [||rs||]
                     head_rec = yld (Record fs sch)
                 in head_rec) >> r $$(parseRow' sch [||rs||]) )||]


    parseRow :: Schema -> QTExp ByteString -> (QTExp ByteString, [QTExp ByteString])
    parseRow [] b = (b, [])
    parseRow [_] b =
      ([|| let res = BC.dropWhile (/= '\n') $$b in BC.tail res ||]
      , [[|| BC.takeWhile (/= '\n') $$b ||]])

    parseRow (_:ss') b =
      let new = [|| let res = BC.dropWhile (/= ',') $$b in BC.tail res ||]
          (final, rs) = parseRow ss' new
      in (final, [|| BC.takeWhile (/= ',') $$b ||] : rs)



printFields :: Fields -> QTExp Res
printFields [] = [|| return () ||]
printFields [x] = [|| print $$x ||]
printFields (x:xs) =
  [||  B.putStr $$x >> $$(printFields xs) ||]

evalPred :: Predicate -> Record -> Code Bool
evalPred predicate rec =
  case predicate of
    Eq a b -> eq (evalRef a rec) (evalRef b rec)
    Ne a b -> neq (evalRef a rec) (evalRef b rec)

evalRef :: Ref -> Record -> Code ByteString
evalRef (Value a) _ = Code [|| a ||]
evalRef (Field name) r = Code (getField name r)


restrict :: Record -> Schema -> Schema -> Record
restrict r newSchema parentSchema =
  Record (map (flip getField r) parentSchema) newSchema

embedFileT :: FilePath -> Code ByteString
embedFileT = Code . unsafeTExpCoerce . embedFile

embedFile :: FilePath -> Q Exp
embedFile fp = (runIO $ B.readFile fp) >>= bsToExp

bsToExp :: B.ByteString -> Q Exp
bsToExp bs = do
    helper <- [| stringToBs |]
    let chars = BC.unpack . BC.tail . (BC.dropWhile (/= '\n')) $ bs
    return $! AppE helper $! LitE $! StringL chars

stringToBs :: String -> B.ByteString
stringToBs = BC.pack

_eq :: Eq a => [QTExp a] -> [QTExp a] -> QTExp Bool
_eq [] [] = [|| True ||]
_eq (v:vs) (v1:v1s) = [|| if $$v == $$v1 then $$(_eq vs v1s) else False ||]
_eq _ _ = [|| False ||]

execOp :: Operator -> (Record -> QTExp Res) -> QTExp Res
execOp op yld =
  case op of
    Scan file sch ->
      processCSV sch (embedFileT file) yld
    Print p       -> execOp p (printFields . fields)
    Filter predicate parent -> execOp parent
      (\rec -> [|| when $$(runCode $ evalPred predicate rec) $$(yld rec) ||] )
    Project newSchema parentSchema parent -> execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in [|| when $$(_eq (getFields keys rec) (getFields keys rec'))
                ($$(yld (Record (fields rec ++ fields rec')
                                (schema rec ++ schema rec')))) ||] ))

runQuery :: Operator -> QTExp Res
runQuery q = execOp (Print q) (\_ -> [|| return () ||])

test :: IO ()
test = do
--  processCSV "data/test.csv" (print . getField "name")
  expr <- runQ $ unTypeQ  $ execOp (Print query) (\_ -> [|| return () ||])
--  expr <- runQ $ unTypeQ  $ power
  putStrLn $ pprint expr


