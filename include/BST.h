#pragma once
#include<iostream>
#include<string>
#include<sstream>
using namespace std;
struct BSTNode
{
    BSTNode(const string& key = string(), const string& value = string())
        : _pLeft(nullptr), _pRight(nullptr), _key(key), _value(value)
    {}
    BSTNode* _pLeft;
    BSTNode* _pRight;
    string _key;
    string _value;
};
class BST
{
public:
    typedef BSTNode Node;
private:
    Node* _root = nullptr;
    string ser(Node* root);
    Node* deser(istringstream& in);
public:
    int size = 0;
    void Insert(const string& key, const string& value);
    Node* Find(const string& key);
    bool Erase(const string& key);
    void _InOrder(Node* root);
    void InOrder();
    void _deleteNode(Node* root);
    Node* _getRoot();
    string serialize();
    void deserialize(string data);
    ~BST();
};
