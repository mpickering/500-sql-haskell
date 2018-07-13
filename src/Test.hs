{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Test where

import Compiler

main = do
  putStrLn ($$(runQuery query))
  putStrLn ($$(runQuery query2))
