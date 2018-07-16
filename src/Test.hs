{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Test where

import Compiler
import qualified LMS as L

main = do
  putStrLn ($$(runQuery query))
  putStrLn ($$(runQuery query2))

  putStrLn ($$(L.runQuery L.query))
  putStrLn ($$(L.runQuery L.query2))
