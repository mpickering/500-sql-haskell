{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveLift #-}
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
import qualified Prelude as P
import Instances.TH.Lift ()

eq :: Eq a => Code a -> Code a -> Code Bool
eq (Code e1) (Code e2) = Code [|| $$e1 == $$e2 ||]

neq :: Eq a => Code a -> Code a -> Code Bool
neq (Code e1) (Code e2) = Code [|| $$e1 /= $$e2 ||]

bc_split :: Code Char -> Code ByteString -> Code [ByteString]
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

data ResRecord = ResRecord { fields_r :: [ByteString], schema_r :: Schema }




getField :: ByteString -> Record -> QTExp ByteString
getField field (Record fs sch) =
  let i = fromJust (elemIndex field sch)
  in  (fs !! i)

getFields :: [ByteString] -> Record -> [QTExp ByteString]
getFields fs r = map (flip getField r) fs

data Operator = Scan FilePath Schema | Project Schema Schema Operator
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

data Scanner = Scanner ByteString

newScanner :: FilePath -> IO Scanner
newScanner fp = Scanner <$>  B.readFile fp

nextLine :: Scanner -> (ByteString, Scanner)
nextLine (Scanner bs) =
  let (fs, rs) = BC.span (/= '\n') bs
  in (fs, Scanner (BC.tail rs))

hasNext :: Scanner -> Bool
hasNext (Scanner bs) = bs /= ""

while ::
  Monoid m =>
  QTExp (t -> Bool) -> QTExp ((t -> IO m) -> t -> IO m) -> QTExp (t -> IO m)
while k b = [|| fix (\r rs -> whenM ($$k rs) ($$b r rs)) ||]

whenM :: Monoid m => Bool -> m -> m
whenM b act = if b then act else mempty

processCSV :: forall m .Monoid m => Schema -> FilePath -> (Record -> QTExp (IO m)) -> QTExp (IO m)
processCSV ss f yld =
  [|| do
        bs <- newScanner f
        $$(rows ss) bs ||]
  where
    rows :: Schema -> QTExp (Scanner -> IO m)
    rows sch = do
      while [|| hasNext ||]
            [|| \r rs -> do
                  let (hs, ts) = nextLine rs
                  ($$(let fs = parseRow sch [||hs||]
                          head_rec = yld (Record fs sch)
                   in head_rec) >> r ts)||]

    -- We can't use the standard |BC.split| function here because
    -- we we statically know how far we will unroll. The result is then
    -- more static as we can do things like drop certain fields if we
    -- perform a projection.
    parseRow :: Schema -> QTExp ByteString -> [QTExp ByteString]
    parseRow [] _ = []
    parseRow [_] b =
      [[|| BC.takeWhile (/= '\n') $$b ||]]
    parseRow (_:ss') b =
      let new = [|| let res = BC.dropWhile (/= ',') $$b in BC.tail res ||]
          rs = parseRow ss' new
      in ([|| BC.takeWhile (/= ',') $$b ||] : rs)



printFields :: Fields -> QTExp Res
printFields [] = [|| return () ||]
printFields [x] = [|| BC.putStrLn $$x ||]
printFields (x:xs) =
  [||  B.putStr $$x >> BC.putStr "," >> $$(printFields xs) ||]

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

stringToBs :: String -> B.ByteString
stringToBs = BC.pack

-- We can unroll (==) if we statically know the length of each argument.
_eq :: Eq a => [QTExp a] -> [QTExp a] -> QTExp Bool
_eq [] [] = [|| True ||]
_eq (v:vs) (v1:v1s) = [|| if $$v == $$v1 then $$(_eq vs v1s) else False ||]
_eq _ _ = [|| False ||]

execOp :: Monoid m => Operator -> (Record -> QTExp (IO m)) -> QTExp (IO m)
execOp op yld =
  case op of
    Scan file sch ->
      processCSV sch file yld
    Filter predicate parent -> execOp parent
      (\rec -> [|| whenM $$(runCode $ evalPred predicate rec) $$(yld rec) ||] )
    Project newSchema parentSchema parent ->
      execOp parent (\rec -> yld (restrict rec newSchema parentSchema ))
    Join left right ->
      execOp left (\rec -> execOp right (\rec' ->
        let keys = schema rec `intersect` schema rec'
        in [|| whenM $$(_eq (getFields keys rec) (getFields keys rec'))
                ($$(yld (Record (fields rec ++ fields rec')
                                (schema rec ++ schema rec')))) ||] ))

runQuery :: Operator -> QTExp Res
runQuery q = execOp q (printFields . fields)


-- We still need to eliminate the binding time abstraction
runQueryL :: Operator -> QTExp (IO [ResRecord])
runQueryL o = execOp o (\r -> [|| return (return $$(spill r)) ||])

spill :: Record -> QTExp ResRecord
spill (Record rs ss) = [|| ResRecord $$(spill2 rs) ss ||]

spill2 :: [QTExp ByteString] -> QTExp [ByteString]
spill2 [] = [|| [] ||]
spill2 (x:xs) = [|| $$x : $$(spill2 xs) ||]

test :: IO ()
test = do
--  processCSV "data/test.csv" (print . getField "name")
  expr <- runQ $ unTypeQ  $ execOp query (printFields . fields)
--  expr <- runQ $ unTypeQ  $ power
  putStrLn $ pprint expr


